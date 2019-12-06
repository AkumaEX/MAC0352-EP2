from leader import leader
import threading
import socket
import json
import sys
import datetime
import time

BUFFER_SIZE = 4096   # tamanho do buffer de recebimento de dados
LEADER_PORT = 30000  # porta de escuta para envio de endereço
L_DATA_PORT = 40000  # porta de escuta para conexão de dados para os workers


class worker():

    """classe responsável pela execução do trabalho"""

    def __init__(self, debug=False):

        self._leader_addr = None  # endereço ipv4 do líder
        self._undone = True  # trabalho não terminado
        self._send_data = None  # dado a ser enviado para o líder
        self._debug = debug

        if self._debug:
            self._file = open('worker_log_{}.txt'.format(
                datetime.datetime.now().strftime('%H_%M_%S')), 'w')

    def run(self):
        while self._undone:
            try:
                if not self._leader_addr:
                    self._connect_to_leader()
                else:
                    self._task_recv_request()
                    self._task_send_request()
            except:
                self._disconnected_from_leader()
        self._print_log('Finished')
        if self._debug:
            self._file.close()

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

        remaining_attempts = 10
        while remaining_attempts:
            self._print_log('Contacting Leader...')
            s.sendto(message.encode(), leader_addr)

            try:
                data, addr = s.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                self._print_log('No response from Leader. Trying again...')
                remaining_attempts -= 1
            else:
                resp = data.decode()
                code = resp.split()[0]
                if code == '200':
                    self._leader_addr = addr[0]
                    log = 'Leader found at: ({})'.format(self._leader_addr)
                    self._print_log(log)
                    break
        s.close()
        if not remaining_attempts:
            self._undone = False

    def _connect_to_leader(self):
        """Realiza a conexão de dados com o líder"""

        self._find_leader_address()
        if self._leader_addr:
            self._data_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self._data_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._data_socket.connect((self._leader_addr, L_DATA_PORT))

            log = 'Connected to Leader ({})'.format(self._leader_addr)
            self._print_log(log)

    def _disconnected_from_leader(self):
        """Procedimentos no caso de desconexão com o líder"""

        self._print_log('Disconnected from ({})'.format(self._leader_addr))
        self._leader_addr = None
        self._data_socket.close()

    def _task_recv_request(self):
        """
        Pedido de tarefa para o líder.
        O pedido é da forma "600 <comentário>"
        A resposta vem da forma "700 <comentário> (send_size)"
        O retorno são os dados da lista ordenada
        """

        try:
            self._send_message_to_leader('600 Give some task')
            self._execute_response()
        except:
            raise

    def _task_send_request(self):
        """
        Realiza um pedido de envio da tarefa pronta para o líder.
        O pedido é da forma "700 <comentário> (send_size)"
        A resposta vem da forma "900 <comentário>"
        """

        try:
            send_size = len(self._send_data)
            message = '700 Sending completed task ({})'.format(send_size)
            self._send_message_to_leader(message)
            self._execute_response()
        except:
            raise

    def _execute_response(self):
        """Executa a resposta do líder"""
        data = self._data_socket.recv(BUFFER_SIZE)
        if data:
            code = self._get_code_from(data)

            if code == '300':  # tornar-se líder
                self._create_leader()
                raise
            elif code == '500':  # trabalho finalizado
                self._undone = False
                raise
            elif code == '700':  # recebeu o tamanho dos dados do líder
                self._recv_task(data)
            elif code == '900':  # pronto para enviar os dados para o líder
                self._send_task()
        else:
            raise

    def _create_leader(self):
        """Cria uma nova thread de líder"""
        time.sleep(3)
        self._print_log('Creating a new Leader')
        t = threading.Thread(target=leader_t, args=(self._debug,))
        t.start()

    def _recv_task(self, data):
        """
        Envia uma mensagem de aceite para receber os dados.
        Recebe os dados e realiza a ordenação das listas.
        """

        try:  # tenta enviar a mensagem de pronto
            self._send_message_to_leader('900 Ready to receive')
        except:
            raise
        else:
            try:
                size = self._get_size_from(data)
                data = self._data_socket.recv(size)
                log = 'Task received from Leader ({})'.format(self._leader_addr)
                self._print_log(log)
                d = json.loads(data)
            except:  # recebeu mensagem ao invés dos dados
                self._task_cancel(data)
            else:  # executa a tarefa
                sorted = self._merge(d['a'], d['b'])

                send_data = dict(index=d['index'],
                                 sorted=sorted,
                                 num_iter=d['num_iter'])
                self._send_data = json.dumps(send_data).encode()

    def _send_task(self):
        """Transfere os dados enfileirados para o líder"""

        try:
            log = 'Sending result to Leader ({})'.format(self._leader_addr)
            self._print_log(log)
            self._data_socket.sendall(self._send_data)
        except:
            self._task_cancel()
            raise

    def _task_cancel(self, data=None):
        """Procedimentos caso a tarefa seja abortada"""

        data = self._data_socket.recv(BUFFER_SIZE) if not data else data
        if data:
            code = self._get_code_from(data)
            if code == '300':  # tornar-se líder
                self._create_leader()
            elif code == '500':  # trabalho finalizado
                self._undone = False

    def _merge(self, a, b):
        """recebe duas listas ordenadas e devolve a intercalação destas"""
        c = []
        i = 0
        j = 0
        while i < len(a) and j < len(b):
            if a[i] <= b[j]:
                c.append(a[i])
                i += 1
            else:
                c.append(b[j])
                j += 1
        while i < len(a):
            c.append(a[i])
            i += 1
        while j < len(b):
            c.append(b[j])
            j += 1
        return c

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
    w = worker()
    w.run()
