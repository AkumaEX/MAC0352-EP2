from list_manager import list_manager
from leader import leader
import threading
import socket
import json
import sys
import datetime

BUFFER_SIZE = 4096   # tamanho do buffer de recebimento de dados
LEADER_PORT = 30000  # porta de escuta do líder para envio de endereço
F_DATA_PORT = 50000  # porta de escuta para conexão de dados do líder


class first():

    """Classe responsável pela entrega e recebimento dos trabalhos"""

    def __init__(self, filename, debug=False):
        self._num_list = list_manager(self._get_num_list(filename))
        self._leader_addr = None  # endereço ipv4 do líder
        self._debug = debug

        if self._debug:
            self._file = open('first_log_{}.txt'.format(
                datetime.datetime.now().strftime('%H_%M_%S')), 'w')

    def run(self):
        while(self._num_list.is_unsorted()):
            try:
                if not self._leader_addr:
                    self._connect_to_leader()
                else:
                    data = self._data_socket.recv(BUFFER_SIZE)
                    if data:
                        code = self._get_code_from(data)
                        if code == '600':
                            self._lists_recv_response()
                        elif code == '700':
                            self._lists_send_response(data)
                    else:
                        raise
            except:
                self._disconnected_from_leader()

        self._send_message_to_leader('500 All done')
        self._data_socket.close()
        self._print_results()
        self._print_log('Finished')
        if self._debug:
            self._file.close()

    def _get_num_list(self, filename):
        try:
            file = open(filename, 'r')
        except OSError:
            sys.stdout.write('Error: {} cannot be opened\n'.format(filename))
        else:
            num_list = file.readlines()
            return list(map(int, num_list))

    def _find_leader_address(self):
        """
        Envia o pedido do endereço do líder.
        O pedido é da forma "100 <comentário>"
        A resposta vem da forma "200 <comentário>"
        """

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.settimeout(1)
        leader_addr = ('<broadcast>', LEADER_PORT)
        message = '100 Searching for Leader'

        remaining_attempts = 5
        while remaining_attempts:
            self._print_log('Contacting Leader...')
            s.sendto(message.encode(), leader_addr)

            try:
                data, addr = s.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                self._print_log('No response from Leader. Trying again...')
                remaining_attempts -= 1
            else:
                code = self._get_code_from(data)
                if code == '200':
                    self._leader_addr = addr[0]
                    log = 'Leader found at: ({})'.format(self._leader_addr)
                    self._print_log(log)
                    break
        s.close()

    def _connect_to_leader(self):
        """Realiza a conexão de dados com o líder"""

        while not self._leader_addr:
            self._find_leader_address()
            if not self._leader_addr:
                self._print_log('Creating a new Leader')
                t = threading.Thread(target=leader_t, args=(self._debug,))
                t.start()
        self._data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._data_socket.connect((self._leader_addr, F_DATA_PORT))
        self._print_log('Connected to Leader ({})'.format(self._leader_addr))

    def _disconnected_from_leader(self):
        """Procedimentos no caso de desconexão com o líder"""

        self._print_log('Disconnected from ({})'.format(self._leader_addr))
        self._leader_addr = None
        self._data_socket.close()

    def _lists_recv_response(self):
        """
        Responde ao pedido de listas pelo líder.
        O pedido vem da forma "600 <comentário>"
        A resposta é da forma "700 <comentário> (send_size)"
        """

        # preparamos as listas a serem enviadas
        index, a, b, num_iter = self._num_list.get_lists()
        send_data = dict(index=index, a=a, b=b, num_iter=num_iter)
        send_data = json.dumps(send_data)
        send_size = len(send_data)

        try:
            # envia o tamanho do buffer primeiro
            message = '700 Request accepted. Send size: ({})'.format(send_size)
            self._send_message_to_leader(message)

            # espera a mensagem de pronto
            data = self._data_socket.recv(BUFFER_SIZE)
            if data:
                code = self._get_code_from(data)
                if code == '900':
                    log = 'Sending lists to Leader ({})'.format(
                        self._leader_addr)
                    self._print_log(log)
                    self._data_socket.sendall(send_data.encode())
            else:  # se não receber os dados
                raise
        except:
            raise

    def _lists_send_response(self, data):
        """
        Responde ao pedido de envio da lista ordenada.
        O pedido vem da forma "700 <comentário> (send_size)"
        A resposta é da forma "900 <comentário>"
        """

        try:
            # envia a resposta de aceite
            self._send_message_to_leader('900 Ready to receive')

            # recebe a lista ordenada
            size = self._get_size_from(data)
            data = self._data_socket.recv(size)
            if data:
                log = 'Lists received from Leader ({})'.format(
                    self._leader_addr)
                self._print_log(log)
                data = json.loads(data.decode())
                self._num_list.set_list(
                    data['index'], data['sorted'], data['num_iter'])

                # se mudou de iteração
                if self._is_new_iter(data['num_iter']):
                    self._change_leader()
                    raise
                else:
                    self._send_message_to_leader('200 Sorted List Received')
            else:
                raise
        except:
            raise

    def _is_new_iter(self, num_iter):
        """Verifica se mudou de iteração"""

        return num_iter != self._num_list.num_iter() and self._num_list.is_unsorted()

    def _change_leader(self):
        """Envia uma mensagem para iniciar a eleição de líder"""

        self._print_log('Initializing Leader Election')
        self._send_message_to_leader('400 Initialize Leader Election')

    def _create_leader(self):
        """Cria uma nova thread de líder"""

        self._print_log('Creating a new Leader')
        t = threading.Thread(target=leader_t)
        t.start()

    def _print_results(self):
        """Imprime o resultado em um arquivo"""

        self._print_log('Printing Results')
        with open('result.txt', 'w') as file:
            for i in self._num_list._num_list:
                file.write('{}\n'.format(i))

    def _print_log(self, message):
        """Imprime uma mensagem de log com carimbo de tempo"""

        if self._debug:
            self._file.write('({}) {}\n'.format(self._get_time(), message))

    def _get_time(self):
        """Retorna o instante no formato <horas:minutos:segundos:milésimos>"""

        return datetime.datetime.now().strftime('%H:%M:%S:%f')

    def _send_message_to_leader(self, message):
        """Envia uma mensagem para o líder"""

        self._print_log('Message to Leader ({}): {}'.format(
            self._leader_addr, message))
        self._data_socket.sendall(message.encode())

    def _get_code_from(self, data):
        """Extrai o código pela resposta do líder"""

        resp = data.decode()
        log = 'Message from Leader ({}): {}'.format(self._leader_addr, resp)
        self._print_log(log)
        return resp.split()[0]

    def _get_size_from(self, data):
        """Extrai o tamanho do buffer pela resposta do líder"""

        resp = data.decode()
        return int(resp[resp.find('(')+1:resp.find(')')])


def leader_t(debug):
    lo = leader(debug)
    lo.run()


if __name__ == "__main__":

    f = first(sys.argv[1])
    f.run()
