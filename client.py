import json
import signal
import socket
import sys


class Client:

    def __init__(self, port):
        self.PORT = int(port)

        self.config_timeout()
        self.start_server()

    def send_change(self):
        # Cria um socket tcp
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:

            port = int(input("A porta do lider:"))

            # Recebe mensagem a ser enviada
            msg_value = input("Insira a mensagem a ser enviada:")

            # Se conecta no servidor
            tcp.connect(('', port))

            msg = {
                'type': 'client',
                'change': msg_value
            }
            msg = json.dumps(msg)
            msg = msg.encode('utf-8')

            # Sends messge
            tcp.sendall(msg)

    def start_server(self):

        print('criando socket')

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
            # Une o socket ao host e a porta
            tcp.bind(('', self.PORT))  # Recebe mensagens de qualquer host

            # Habilita o servidor a aceitar 5 conexaoes
            tcp.listen(5)

            while True:

                try:
                    conn, address = tcp.accept()

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

                except Exception:
                    self.send_change()
                    self.config_timeout()

    def config_timeout(self):
        signal.signal(signal.SIGALRM, self.send_change)
        signal.alarm(10)

if __name__== "__main__":

    port = sys.argv[1]
    client = Client(port)
