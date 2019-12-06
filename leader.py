import threading
import socket
import json
import sys
import datetime
import select
import queue
import operator
import time

BUFFER_SIZE = 4096   # tamanho do buffer de recebimento de dados
LEADER_PORT = 30000  # porta de escuta para envio de endereço
L_DATA_PORT = 40000  # porta de escuta para conexão de dados para os workers
F_DATA_PORT = 50000  # porta de escuta para conexão de dados para o primeiro


class leader():

    """Classe responsável pela divisão dos trabalhos"""

    def __init__(self, debug=False):

        # sockets para transmissão de endereço da conexão de dados
        self._conn_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._conn_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._conn_socket.setblocking(0)
        self._conn_socket.bind(('', LEADER_PORT))

        # sockets de escuta para conexão de dados com os workers
        self._data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._data_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._data_socket.setblocking(0)
        self._data_socket.bind(('', L_DATA_PORT))
        self._data_socket.listen(10)

        # socket de conexão de dados exclusivo com o primeiro computador
        self._fcom_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._fcom_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._fcom_socket.bind(('', F_DATA_PORT))
        self._fcom_socket.listen(1)

        # estrutura de dados para gerenciamento de sockets
        self._inputs = [self._conn_socket,
                        self._data_socket, self._fcom_socket]
        self._outputs = []
        self._message_queues = {}  # fila das mensagens recebidas
        self._sending_queues = {}  # fila para entrega dos dados
        self._num_tasks_done = {}  # número de tarefas realizadas

        self._fcom = None  # socket do primeiro computador
        self._undone = True  # trabalho não terminado
        self._send_data = None  # dado a ser enviado
        self._first_addr = None  # endereço ipv4 do primeiro computador
        self._debug = debug

        if self._debug:
            self._file = open('leader_log_{}.txt'.format(
                datetime.datetime.now().strftime('%H_%M_%S')), 'w')

    def run(self):
        while self._undone:

            readable, writable, exceptional = select.select(
                self._inputs, self._outputs, self._inputs)

            try:
                self._handle_inputs(readable)
                if self._first_addr:  # somente com a conexão com o primeiro
                    self._handle_outputs(writable)
                    self._handle_conditions(exceptional)
            except:
                self._disconnected_from_first()

        self._print_log('Finished')
        if self._debug:
            self._file.close()

    def _handle_inputs(self, readable):
        """Manipula sockets prontos para leitura"""

        for s in readable:
            if s is self._conn_socket:  # pedido de endereço
                self._send_address()
            elif s is self._data_socket:  # pedido de conexões dos workers
                conn, addr = self._data_socket.accept()
                self._print_log('Connected to Worker ({})'.format(addr[0]))
                self._inputs.append(conn)
                self._message_queues[conn] = queue.Queue()
                self._sending_queues[conn] = queue.Queue()
                self._num_tasks_done[conn] = 0
            elif s is self._fcom_socket:  # pedido de conexão do primeiro
                self._fcom, self._first_addr = self._fcom_socket.accept()
                log = 'Connected to First ({})'.format(self._first_addr[0])
                self._print_log(log)
            else:  # comandos dos workers
                data = s.recv(BUFFER_SIZE)
                if data:
                    self._message_queues[s].put(data)
                    if s not in self._outputs:
                        self._outputs.append(s)
                else:  # conexão fechada, remover o socket do worker
                    self._remove_worker(s)

    def _handle_outputs(self, writable):
        """Manipula sockets prontos para escrita"""

        for s in writable:
            try:
                data = self._message_queues[s].get_nowait()
            except queue.Empty:
                self._outputs.remove(s)
            else:
                code = self._get_code_from(data)
                log = 'Message from Worker ({}): {}'.format(
                    s.getpeername()[0], data.decode())
                self._print_log(log)
                if code == '600':
                    self._task_recv_response(s)
                elif code == '700':
                    self._task_send_response(s, data)
                elif code == '900':
                    self._send_task(s)

    def _handle_conditions(self, exceptional):
        """Manipula sockets com condições excepcionais"""

        for s in exceptional:
            self._inputs.remove(s)
            if s in self._outputs:
                self._outputs.remove(s)
            s.close()

    def _send_address(self):
        """
        Verifica se há um pedido de envio de endereço pelo worker.
        e envia uma resposta para o remetente.
        O pedido vem da forma "100 <comentário>" dos workers
        A resposta é da forma "200 <comentário>" para workers
        """
        data, addr = self._conn_socket.recvfrom(BUFFER_SIZE)
        if data:
            code = self._get_code_from(data)
            if code == '100':
                self._conn_socket.sendto('200 ok'.encode(), addr)

    def _task_recv_response(self, s):
        """
        Responde ao pedido de tarefa pelo worker.
        O pedido vem da forma "600 <comentário>"
        A resposta é da forma "700 <comentário> (send_size)"
        """
        #  recebe as listas do primeiro computador
        self._lists_recv_request()
        size = len(self._send_data)
        try:
            # envia o tamanho do buffer primeiro e depois a tarefa
            message = '700 Request accepted. Send size: ({})'.format(size)
            self._send_message_to_worker(message, s)
            self._sending_queues[s].put(self._send_data)
        except:
            self._remove_worker(s)

    def _task_send_response(self, s, data):
        """
        Responde ao pedido de envio da tarefa pronta para o Worker.
        Recebe a tarefa pronta do worker e envia para o primeiro
        O pedido vem da forma "700 <comentário> (send_size)"
        A resposta é da forma "900 <comentário>"
        """
        try:
            # envia a resposta de aceite para o worker
            self._send_message_to_worker('900 Ready to receive', s)

            # recebe a tarefa pronta do worker
            size = self._get_size_from(data)
            self._send_data = s.recv(size)
            log = 'Sorted list received from Worker ({})'.format(
                s.getpeername()[0])
            self._print_log(log)

            if self._send_data:
                # envia a lista ordenada para o primeiro
                self._lists_send_request(self._send_data)
                self._num_tasks_done[s] += 1
            else:
                raise
        except:
            self._remove_worker(s)

    def _send_task(self, s):
        """Transfere os dados enfileirados para o socket s"""
        try:
            log = 'Sending task to Worker ({})'.format(s.getpeername()[0])
            self._print_log(log)
            s.sendall(self._sending_queues[s].get())
        except:
            self._remove_worker(s)

    def _lists_recv_request(self):
        """
        Pedido de listas para o primeiro computador.
        O pedido é da forma "600 <comentário>"
        A resposta vem da forma "700 <comentário> (send_size)"
        O retorno são os dados das listas
        """

        try:
            self._send_message_to_first('600 Give some lists')
            self._execute_response()
        except:
            raise

    def _lists_send_request(self, send_data):
        """
        Pedido de envio de lista ordenada para o primeiro computador.
        O pedido é da forma "700 <comentário> (send_size)"
        A resposta vem da forma "900 <comentário>"
        """
        try:
            send_size = len(send_data)
            message = '700 Sending sorted list ({})'.format(send_size)
            self._send_message_to_first(message)
            self._execute_response()
        except:
            raise

    def _execute_response(self):
        """Executa a resposta do primeiro computador"""

        data = self._fcom.recv(BUFFER_SIZE)
        if data:
            code = self._get_code_from(data)
            log = 'Message from First ({}): {}'.format(
                self._first_addr[0], data.decode())
            self._print_log(log)

            if code == '400':  # eleição de líder
                self._leader_election()
                raise
            elif code == '500':  # trabalho concluído
                self._all_done()
                raise
            elif code == '700':  # pronto para receber os dados do primeiro
                self._recv_list(data)
            elif code == '900':  # pronto para enviar os dados para o primeiro
                self._send_list()
        else:
            raise

    def _leader_election(self):
        """
        Escolhe o worker que realizou mais tarefas e envia uma mensagem
        informando que se tornou o líder da próxima iteração.
        """
        try:
            self._print_log('Initalizing Leader Election')
            # escolhe o worker e envia a mensagem
            s = max(self._num_tasks_done.items(),
                    key=operator.itemgetter(1))[0]
            self._send_message_to_worker('300 Become a Leader', s)
        except:
            self._remove_worker(s)
            self._leader_election()
        else:
            log = 'New Leader Elected ({})'.format(s.getpeername()[0])
            self._print_log(log)
            self._close_all_connections()

    def _all_done(self):
        """
        Envia uma mensagem de trabalho concluído para todos os Workers
        e fecha todas as conexões
        """
        self._send_message_to_workers('500 All done')
        self._close_all_connections()

    def _send_message_to_workers(self, message):
        """Envia uma mensagem para todos os workers"""

        workers = [
            s for s in self._inputs if s is not self._conn_socket and s is not self._data_socket and s is not self._fcom_socket]

        for s in workers:
            try:
                self._send_message_to_worker(message, s)
            except:
                self._remove_worker(s)
        time.sleep(3)

    def _close_all_connections(self):
        """Fecha todas as conexões"""

        for s in self._inputs:
            log = 'Closing connection ({})'.format(s.getpeername())
            self._print_log(log)
            s.close()

        message = 'Closing connection to First ({})'.format(
            self._first_addr[0])
        self._print_log(message)
        self._fcom.close()
        self._first_addr = None
        self._undone = False

    def _recv_list(self, data):
        """Recebe os dados das listas do primeiro computador"""
        try:
            # envia a mensagem de pronto
            self._send_message_to_first('900 Ready to receive')

            # recebe os dados
            size = self._get_size_from(data)
            data = self._fcom.recv(size)
            if data:
                self._send_data = data
                log = 'Task received from First ({})'.format(
                    self._first_addr[0])
                self._print_log(log)
            else:
                raise
        except:
            raise

    def _send_list(self):
        """Envia os dados da lista ordenada para o primeiro computador"""

        try:
            log = 'Sending sorted list to First ({})'.format(
                self._first_addr[0])
            self._print_log(log)
            self._fcom.sendall(self._send_data)
            self._execute_response()
        except:
            self._disconnected_from_first()

    def _disconnected_from_first(self):
        """Procedimentos no caso de desconexão com o primeiro computador"""

        log = 'Disconnected from First ({})'.format(self._first_addr[0])
        self._print_log(log)
        self._fcom.close()
        self._first_addr = None
        self._undone = False

    def _remove_worker(self, s):
        """Remove o socket do worker"""

        log = 'Removing disconnected Worker ({})'.format(s.getpeername())
        self._print_log(log)

        self._inputs.remove(s)
        del self._message_queues[s]
        del self._sending_queues[s]
        del self._num_tasks_done[s]
        if s in self._outputs:
            self._outputs.remove(s)
        s.close()

    def _print_log(self, message):
        """Imprime uma mensagem de log com carimbo de tempo"""

        if self._debug:
            self._file.write('({}) {}\n'.format(self._get_time(), message))

    def _get_time(self):
        """Retorna o instante no formato <horas:minutos:segundos:milésimos>"""

        return datetime.datetime.now().strftime('%H:%M:%S:%f')

    def _send_message_to_first(self, message):
        """Envia uma mensagem para o primeiro computador"""

        log = 'Message to First ({}): {}'.format(self._first_addr[0], message)
        self._print_log(log)
        self._fcom.sendall(message.encode())

    def _send_message_to_worker(self, message, s):
        """Envia uma mensagem para o Worker s"""

        log = 'Message to Worker ({}): {}'.format(s.getpeername()[0], message)
        self._print_log(log)
        s.sendall(message.encode())

    def _get_code_from(self, data):
        """Extrai o código pela mensagem de resposta"""

        resp = data.decode()
        return resp.split()[0]

    def _get_size_from(self, data):
        """Extrai o tamanho do buffer pela mensagem de resposta"""

        resp = data.decode()
        size = int(resp[resp.find('(')+1:resp.find(')')])
        return max(size, BUFFER_SIZE)


if __name__ == "__main__":

    l = leader()
    l.run()
