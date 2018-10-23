import socket
import node


class Client:

    def __init__(self):
        pass

    def send_change(self, server):
        # Cria um socket tcp
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
            # Se conecta no servidor
            tcp.connect(('', server.PORT))

            # Recebe mensagem a ser enviada
            msg_value = input("Insira a mensagem a ser enviada:")

            msg = {'change': msg_value}

            # Sends messge
            tcp.sendall(bytearray(msg, 'utf-8'))

            # Response from server
            data = tcp.recv(1024)

        # Imprime os dados recebidos
        print('Recebido: ', repr(data))
