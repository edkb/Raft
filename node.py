import random
import time
import socket
import signal
import sys
import json
import my_exceptions


class ServerNode:

    state = ['Follower', 'Candidate', 'Leader']

    nodos = {
        'node1': {'name': 'a', 'port': 5001},
        'node2': {'name': 'b', 'port': 5002},
        'node3': {'name': 'c', 'port': 5003},
        'node4': {'name': 'd', 'port': 5004},
        'node5': {'name': 'e', 'port': 5005},
    }

    def __init__(self, node_id, node_port):

        self._name = node_id        # Identification of the server node
        self._state = 'Follower'    # Every server starts as follower

        self.PORT = int(node_port)   # Arbitrary port for the server
        self._conn = None
        self._address = None

        # The election timeout is the amount of time a follower waits until becoming a candidate.
        self._election_timeout = self.get_election_timeout()  # Sets node election timeout
        self._votes_in_term = 0      # Votes received by the candidate in a given election term
        self._heartbeat_timeout = self.get_hearbeat_timeout()  # Must be less then election timeout

        # Persistent state on all servers
        self._current_term = 0   # Latest term server has seen
        self._voted_for = None   # Candidate id that received vote in current term
        self._log = []           # log entries; each entry contains command for state machine, and term when entry was received by leader

        # Volatile state on all servers
        self._commit_index = 0   # Index of highest log entry known to be committed
        self._last_applied = 0   # Index of highest log entry applied to state machine

        # Volatile state on leaders, for each server
        self._next_index  = 0    # Index of the next log entry to send to that server, also known as last log index
        self._match_index = 0    # Index of highest log entry known to be replicated on server, also known as last log term

        self._leader = None
        self.start()

    def start(self):

        print(f'Starting node {self._name} listening on port {self.PORT} as {self._state}')

        self.config_timeout()
        self.receive_msg()

    def config_timeout(self):

        if self._state in ['Follower', 'Candidate']:
            print(f'Configured follower node {self._name} with election timeout to: {self._election_timeout}')
            signal.signal(signal.SIGALRM, self.election_timeout_handler)
            signal.alarm(self._election_timeout)

        elif self._state == 'Leader':
            print(f'Configured candidate node {self._name} with heartbeat timeout to: {self._heartbeat_timeout}')
            signal.signal(signal.SIGALRM, self.heartbeat_timeout_handler)
            signal.alarm(self._heartbeat_timeout)

    def election_timeout_handler(self, signum, frame):
        print("Reached election timeout!")
        raise my_exceptions.ElectionException('Election')

    def heartbeat_timeout_handler(self, signum, frame):
        print("Reached hearbeat timeout!")
        raise my_exceptions.HeartbeatException('Hearbeat')

    # Remote procedure call
    def request_vote(self, node_port):

        msg = {
            'term': self._current_term,
            'candidate_id': self._name,
            'last_log_index': self._last_applied,
            'last_log_term': self._commit_index
        }

        reply = self.send_msg(msg, node_port)

        # Imprime os dados recebidos
        print('Recebido: ', repr(reply))

        if reply['canidate_id'] == self.__name:
            self._votes_in_term += 1
            print('Votes in term')

        # Once a candidate has a majority of votes it becomes leader.
        if self._votes_in_term > 2:
            self.state = 'Leader'

    def reply_vote(self, msg):
        """
        If the receiving node hasn't voted yet in this term then it votes for the candidate...
        :param msg:
        :return:
        """
        if msg['term'] > self._current_term:

            self._state = "Follower"  # Becomes follower again if term is outdated
            print('Follower')

            self._current_term = msg['term']
            self._voted_for = msg['candidate_id']
            reply_vote = {
                'candidate_id': msg['candidate_id']
            }
            self._election_timeout = self.get_election_timeout()  # ...and the node resets its election timeout.
            self.config_timeout()
            return json.dumps(reply_vote)

        else:
            reply_vote = {
                'candidate_id': self._voted_for
            }
            return json.dumps(reply_vote)

    def vote_in_candidate(self, candidate):
        pass

    def commit(self):
        pass

    def notify_followers(self):
        pass

    # Remote procedure call
    def append_entries(self):
        """
        Invoked by leader to replicate log entries (§5.3);
        Also used as heartbeat

        :return:
        """
        # Refreshes heartbeat
        self._heartbeat_timeout = self.get_hearbeat_timeout()
        self.config_timeout()

        for node, value in self.nodos.items():
            if value['name'] != self._name:
                try:
                    print(f'Leader trying do append entry for {node}: {value["name"]}')
                    msg = {
                        'type': 'apn_en',
                        'term': self._current_term,
                        'leader_id': self._name,
                        'leader_port': self.PORT,  # AppendEntries requests include the network address of the leader
                        'prev_log_index': self._last_applied,
                        'prev_log_term': self._commit_index,
                        'leader_commit': None
                    }
                    msg = json.dumps(msg)

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

                        # Connects to server destination
                        tcp.connect(('', value['port']))

                        print(f'Send app entry to node {node}: {value["name"]}')

                        # Envia mensagem
                        tcp.sendall(msg.encode('utf-8'))

                        # Recebe dados do servidor
                        reply = tcp.recv(1024).decode('utf-8')

                        print('Append reply: ', reply)

                except Exception as e:
                    print(e)

    def start_election(self):
        """
        Starts a new election term
        :return:
        """
        self._state = 'Candidate'
        self._current_term += 1
        self.config_timeout()

        print(f'Node {self._name} becomes candidate')
        print('Current term:',  self._current_term)
        self._voted_for = self._name
        self._votes_in_term = 1

        # for all nodes in cluster
        for node, value in self.nodos.items():
            time.sleep(0.1)
            if value['name'] != self._name:
                print(f'Trying to connect {node}: {value["name"]}')
                msg = {
                    'type': 'req_vote',
                    'term': self._current_term,
                    'candidate_id': self._name,
                    'last_log_index': self._last_applied,
                    'last_log_term': self._commit_index
                }
                msg = json.dumps(msg)

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

                        tcp.settimeout(0.5)

                        # Connects to server destination
                        tcp.connect(('', value['port']))

                        print(f'Requesting vote to node {node}: {value["name"]}')

                        # Envia mensagem
                        tcp.sendall(msg.encode('utf-8'))

                        # Recebe dados do servidor
                        reply = tcp.recv(1024).decode('utf-8')

                        if not reply:
                            print("Reply not recieved")
                            break

                        reply = json.loads(reply)

                        for key, value in reply.items():

                            if value == self._name:
                                self._votes_in_term += 1
                                print('Votes in term', self._votes_in_term)

                            # Once a candidate has a majority of votes it becomes leader.
                            if self._votes_in_term > 2:
                                self._state = 'Leader'
                                print(f'Node {self._name} becomes {self._state}')
                                self.append_entries()

                except TimeoutError as te:
                    print(te.args[0])
                    tcp.close()
                    continue

                except Exception as e:
                    tcp.close()
                    print(e)
                    continue

                except KeyboardInterrupt:
                    raise SystemExit()
        tcp.close()

    def send_msg(self, msg, node_port, host='localhost'):

        print(f'Sending: {msg} to node in port {node_port}')
        # Creates tcp socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            # Connects to server destination
            tcp.connect((host, node_port))

            # Envia mensagem
            tcp.sendall(msg.encode('utf-8'))

            # Recebe dados do servidor
            data = tcp.recv(1024)

        return data

    def conn_loop(self, conn, address):
            with conn:
                print('Connected by', address)

                # Recebe os dados do cliente
                msg = conn.recv(1024)

                # Se der algo errado com os dados, sai do loop
                if not msg:
                    print('Nothing recieved')
                    conn.close()
                    return

                msg = msg.decode('utf-8')
                msg = json.loads(msg)

                # Imprime os dados recebidos
                print('Msg recieved: ', msg)

                # Envia para o cliente os dados recebidos
                # conn.sendall(data)

                # If it is a message sent from a client
                if msg['type'] == 'client':

                    # Only the leader handles it
                    if self.state == 'Leader':  # This process is called Log Replication
                        # change goes to the leader
                        self._log.append(msg['change'])  # Each change is added as an entry in the nodes's log
                        # This log entry is currently uncommitted so it won't update the node's value.

                        self.append_entries(
                            msg)  # To commit the entry the node first replicates it to the follower nodes...
                        # Then the leader waits until a majority of nodes have written the entry.
                        # The entry is now committed on the leader node and the node state is "X"
                        # The leader then notifies the followers that the entry is committed.
                        # The cluster has now come to consensus about the system state.

                    # If a follower receives a message from a client the it must redirect to the leader
                    else:
                        self.send_msg(msg, port)

                # If it is a append entry message from the leader
                elif msg['type'] == 'apn_en':
                    self._election_timeout = self.get_election_timeout()
                    self.config_timeout()
                    self._state = "Follower"

                    ack_msg = {
                        'client_id': self._name,
                        'term': self._current_term,
                        'type': 'ack_append_entry'
                    }
                    reply = json.dumps(ack_msg)
                    conn.sendall(reply.encode('utf-8'))

                elif msg['type'] == 'heart_beat':
                    reply = {
                        'type': 'heart_beat_reply',
                        'client_if': self._name
                    }
                    conn.sendall(reply.encode('utf-8'))

                elif msg['type'] == 'req_vote':

                    if msg['term'] > self._current_term:

                        self._state = "Follower"  # Becomes follower again if term is outdated
                        print('Follower')

                        self._current_term = msg['term']
                        self._voted_for = msg['candidate_id']
                        reply_vote = {
                            'candidate_id': msg['candidate_id']
                        }
                        self._election_timeout = self.get_election_timeout()  # ...and the node resets its election timeout.
                        self.config_timeout()

                    else:
                        reply_vote = {
                            'candidate_id': self._voted_for
                        }

                    reply_msg = json.dumps(reply_vote)
                    print(f'Replying to {msg["candidate_id"]}')
                    conn.sendall(reply_msg.encode('utf-8'))

    def receive_msg(self):

                print('criando socket')
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

                    # Une o socket ao host e a porta
                    tcp.bind(('', self.PORT))  # Recebe mensagens de qualquer host

                    # Habilita o servidor a aceitar 5 conexaoes
                    tcp.listen(5)

                    while True:

                        try:
                            # Aceita uma conexao. Retorna o objeto do socket (conn) e endereco do cliente (address)
                            self._conn, self._address = tcp.accept()

                            print('socket criado')
                            self.conn_loop(self._conn, self._address)

                        except my_exceptions.ElectionException:
                            print('Starting new election')
                            self.start_election()

                        except my_exceptions.HeartbeatException:
                            print('Appending entries')
                            self.append_entries()

    def reply_append_entry(self, append_entry_msg):
        """
        An entry is committed once a majority of followers acknowledge it...
        :param append_entry_msg:
        :return:
        """
        # TODO: Acknowledge message
        ack_msg = {
            'client_id': self._name,
            'term': self._current_term,
            'type': 'ack_append_entry'
        }
        self.send_msg(ack_msg, append_entry_msg['port'])

    def get_election_timeout(self):
        """
        Set a new election timeout for follower node

        :return: timeout between 3 and 5 seconds
        """
        election_timeout = round(random.uniform(5, 8))
        print(f'Node {self._name} have new election timeout of {election_timeout}')
        return election_timeout

    def get_hearbeat_timeout(self):
        """
         Set a hearbeat  timeout for leader node
        :return: timeout of one seconds
        """
        return 1


if __name__ == "__main__":

    name = sys.argv[1]
    port = sys.argv[2]

    server_node = ServerNode(name, port)
