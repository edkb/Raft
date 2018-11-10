import random
import time
import socket
import signal
import sys
import json


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

        # The election timeout is the amount of time a follower waits until becoming a candidate.
        self._election_timeout = self.set_election_timeout()  # Sets node election timeout
        self._votes_in_term = 0      # Votes received by the candidate in a given election term
        self._heartbeat_timeout = 2  # Must be less then election timeout

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

    def idle(self):
        self.receive_msg()
        '''
        if timeout_type == 'Election':
            print('Starting a new election')
            # After the election timeout the follower becomes a candidate and starts a new election term...
            self.start_election()

        elif timeout_type == 'Hearbeat':  # These messages are sent in intervals specified by the heartbeat timeout.
            #  ...then the change is sent to the followers on the next heartbeat.
            self.append_entries()      # The leader begins sending out Append Entries messages to its followers.
        '''

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
        self.start_election()

    def heartbeat_timeout_handler(self, signum, frame):
        print("Reached hearbeat timeout!")
        raise TimeoutError('Heartbeat')

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

            self._current_term = msg['term']
            self._voted_for = msg['candidate_id']
            reply_vote = {
                'candidate_id': msg['candidate_id']
            }
            self.set_election_timeout()  # ...and the node resets its election timeout.
            return json.dumps(reply_vote)

        else:
            if self._voted_for is None:
                reply_vote = {
                    'candidate_id': msg['candidate_id']
                }
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

    def set_election_timeout(self):
        """
        Set a new election timeout for follower node

        :return: timeout between 3 and 5 seconds
        """
        return round(random.uniform(3, 5))

    # Remote procedure call
    def append_entries(self):
        """
        Invoked by leader to replicate log entries (§5.3);
        Also used as heartbeat

        :return:
        """
        msg = {
            'term': self._current_term,
            'leader_id': self._name,
            'leader_port': self.PORT,  # AppendEntries requests include the network address of the leader
            'prev_log_index': self._last_applied,
            'prev_log_term': self._commit_index,
            'leader_commit': None
        }

        self.send_msg(msg)

        if time.time() <= self._heartbeat_timeout:
            self.send_entry()

    def send_entry(self):
        pass

    def start_election(self):
        """
        Starts a new election term
        :return:
        """
        self._state = 'Candidate'
        self._current_term += 1
        self.config_timeout()

        print(f'Node {self._name} becomes candidate')
        self._voted_for = self._name
        self._votes_in_term = 1

        try:
            # for all nodes in cluster
            for node, value in self.nodos.items():
                time.sleep(0.1)
                if value['name'] != self._name:
                    try:

                        msg = {
                            'type': 'req_vote',
                            'term': self._current_term,
                            'candidate_id': self._name,
                            'last_log_index': self._last_applied,
                            'last_log_term': self._commit_index
                        }
                        msg = json.dumps(msg)

                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

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
                                    print('Votes in term', self._votes_in_term )

                                # Once a candidate has a majority of votes it becomes leader.
                                if self._votes_in_term > 2:
                                    self._state = 'Leader'
                                    print(f'Node {self._name} becomes {self._state}')
                                    self.receive_msg()


                    except Exception as e:
                        print(e)

                    except KeyboardInterrupt:
                        raise SystemExit()

        except:
            return

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

    def receive_msg(self):

        while True:

            # Cria um socket tcp
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

                # Une o socket ao host e a porta
                tcp.bind(('', self.PORT))  # Recebe mensagens de qualquer host

                # Habilita o servidor a aceitar 5 conexaoes
                tcp.listen(5)

                # Aceita uma conexao. Retorna o objeto do socket (conn) e endereco do cliente (address)
                conn, address = tcp.accept()

                print('socket criado')

                with conn:
                    print('Connected by', address)

                    while True:

                        # Recebe os dados do cliente
                        msg = conn.recv(1024).decode('utf-8')

                        # Se der algo errado com os dados, sai do loop
                        if not msg:
                            print('Something went wrong')
                            break

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

                                self.append_entries(msg)  # To commit the entry the node first replicates it to the follower nodes...
                                # Then the leader waits until a majority of nodes have written the entry.
                                # The entry is now committed on the leader node and the node state is "X"
                                # The leader then notifies the followers that the entry is committed.
                                # The cluster has now come to consensus about the system state.

                            # If a follower receives a message from a client the it must redirect to the leader
                            else:
                                self.send_msg(msg, port)

                        # If it is a append entry message from the leader
                        elif msg['type'] == 'apn_en':
                            self.reply_append_entry(msg)   # Followers must then respond to each Append Entries message.
                            # TODO: Write entry and reply to leader
                            pass  # deal with received append entries

                        elif msg['type'] == 'heart_beat':
                            reply = {
                                'type': 'heart_beat_reply',
                                'client_if': self._name
                                }
                            conn.sendall(reply.encode('utf-8'))

                        elif msg['type'] == 'req_vote':
                            reply_msg = self.reply_vote(msg)
                            print(f'Replying to {msg["candidate_id"]}')
                            conn.sendall(reply_msg.encode('utf-8'))


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


if __name__ == "__main__":

    name = sys.argv[1]
    port = sys.argv[2]

    server_node = ServerNode(name, port)
