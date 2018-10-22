import random
import time
import socket
import signal
from contextlib import contextmanager


class ServerNode:

    state = ['Follower', 'Candidate', 'Leader']

    nodos = {
        'node1': {'id': 'a', 'port': 5001},
        'node1': {'id': 'b', 'port': 5002},
        'node1': {'id': 'c', 'port': 5003},
        'node1': {'id': 'd', 'port': 5004},
        'node1': {'id': 'e', 'port': 5005},
    }

    def __init__(self, node_id, node_port):

        self._name = node_id   # Identification of the server node
        self._state = 'Follower'  # Every server starts as follower

        self._PORT = node_port  # Porta arbitraria para o servidor

        # The election timeout is the amount of time a follower waits until becoming a candidate.
        self._election_timeout = random.uniform(150, 300)  # Random value from 150 to 300 milliseconds

        self._votes_in_term = 0  # Votes received by the candidate in a given election term
        self._heartbeat_timeout = 100  # Must be less then election timeout

        # Persistent state on all servers
        self._current_term = 0   # Latest term server has seen
        self._voted_for = None   # Candidate id that received vote in current term
        self._log = []           # log entries; each entry contains command for state machine, and term when entry was received by leader

        # Volatile state on all servers
        self._commit_index = 0   # Index of highest log entry known to be committed
        self._last_applied = 0   # Index of highest log entry applied to state machine

        # Volatile state on leaders, for each server
        self._next_index = 0     # Index of the next log entry to send to that server, also known as last log index
        self._match_index = 0    # Index of highest log entry known to be replicated on server, also known as last log term

    @contextmanager
    def config_timeout(self):

        if self._state == 'Follower':
            signal.signal(signal.SIGALRM, self.election_timeout_handler())
            signal.alarm(self._election_timeout)

        elif self.state == 'Candidate':
            signal.signal(signal.SIGALRM, self.election_timeout_handler())
            signal.alarm(self._election_timeout)

            try:
                yield

            except TimeoutError as err:

                if err.args == 'Election':
                    # After the election timeout the follower becomes a candidate and starts a new election term...
                    self.be_candidate()

                elif err.args == 'Hearbeat':  # These messages are sent in intervals specified by the heartbeat timeout.
                   self.append_entries()      # The leader begins sending out Append Entries messages to its followers.


            finally:
                # Unregister the signal so it won't be triggered
                # if the timeout is not reached.
                signal.signal(signal.SIGALRM, signal.SIG_IGN)

    def be_candidate(self):
        self._state = 'Candidate'
        # for nodes in cluter:
        self.request_vote()

        # Starts new election term
        # Votes for itself
        # Sends request vote messages for other nodes

    def election_timeout_handler(self):
        print("Reached election timeout!")

        raise TimeoutError('Election')

    def heartbeat_timeout_handler(self):
        print("Reached hearbeat timeout!")
        raise TimeoutError('Heartbeat')

    # Remote procedure call
    def request_vote(self):

        msg = {
            'term': self._current_term,
            'candidate_id': self._name,
            'last_log_index': self._last_applied,
            'last_log_term': self._commit_index
        }

        reply = self.send_msg(msg)

        # Imprime os dados recebidos
        print('Recebido: ', repr(reply))

        # Once a candidate has a majority of votes it becomes leader.
        if self._votes_in_term > 2:
            self.state = 'Leader'

    def reply_vote(self, msg):
        """
        If the receiving node hasn't voted yet in this term then it votes for the candidate...
        :param msg:
        :return:
        """
        if self._voted_for is not None:
            if msg['term'] == self._current_term:
                reply_vote = {'candidate_id': msg['candidate_id']}
                self.send_msg(reply_vote)
                self._election_timeout = random.uniform(150, 300)  # ...and the node resets its election timeout.

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
        msg = {
            'term': self._current_term,
            'leader_id': self._name,
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
        self._current_term += 1
        self.request_vote()

    def send_msg(self, msg, dest=None):
        # Cria um socket tcp
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
            # Se conecta no servidor
            tcp.connect((self.HOST, self._PORT))

            # Envia mensagem
            tcp.sendall(bytearray(msg, 'utf-8'))
            tcp.sendto()

            # Recebe dados do servidor
            data = tcp.recv(1024)

            return data

    def receive_msg(self):

        # Cria um socket tcp
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            # Une o socket ao host e a porta
            tcp.bind(('', self._PORT))  # Recebe mensagens de qualquer host

            # Habilita o servidor a aceitar uma conexao
            tcp.listen(1)

            # Aceita uma conexao. Retorna o objeto do socket (conn) e endereco do cliente (address)
            conn, address = tcp.accept()

            with conn:
                print('Connected by', address)

                while True:

                    # Refreshes timeout
                    with self.config_timeout():

                        # Recebe os dados do cliente
                        msg = conn.recv(1024)

                        # Se der algo errado com os dados, sai do loop
                        if not msg: break

                        # Envia para o cliente os dados recebidos
                        # conn.sendall(data)

                        if msg['type'] == 'client':
                            if self.state == 'Leader':  # This process is called Log Replication
                                # change goes to the leader
                                # Each change is added as an entry in the nodes's log
                                # This log entry is currently uncommitted so it won't update the node's value.

                                self.append_entries(msg)  # To commit the entry the node first replicates it to the follower nodes...
                                # Then the leader waits until a majority of nodes have written the entry.
                                # The entry is now committed on the leader node and the node state is "X"
                                # The leader then notifies the followers that the entry is committed.
                                # The cluster has now come to consensus about the system state.
                            else:
                                self.send_msg(msg)

                        if msg['type'] == 'apn_en':  # Followers then respond to each Append Entries message.
                            # Write entry and reply to leader
                            pass  # deal with received append entries

                        elif msg['type'] == 'req_vote':
                            self.reply_vote(msg)

                        # Imprime os dados recebidos
                        print(msg)


if __name__ == "__main__":

    name = input("Enter the name of the node: ")
    port = input("Enter the port of the node: ")

    server_node = ServerNode(name, port)
