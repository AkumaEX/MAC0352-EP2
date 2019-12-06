class list_manager():

    def __init__(self, num_list):
        self._num_list = num_list  # a lista de números
        self._list_size = len(num_list)  # tamanho da lista
        # status: 0 (não enviado), 1 (enviado), 2 (recebido)
        self._status = [0] * self._list_size
        self._send_size = 1  # tamanho de cada lista de envio
        self._iter = 0  # iteração atual

    def get_lists(self):
        """devolve o índice e duas listas ordenadas e o número da iteração"""
        try:
            # buscamos por blocos ainda não enviados
            index = self._status.index(0)
        except ValueError:
            # buscamos por blocos ainda não recebidos e enviamos novamente
            index = self._status.index(1)

        # preparamos duas listas
        start, finish = index, index + self._send_size
        a = self._num_list[start:finish]
        start, finish = index + self._send_size, index + 2 * self._send_size
        b = self._num_list[start:finish]

        # marcamos os elementos como enviado
        start = index
        finish = self._list_size \
            if self._list_size < index + 2 * self._send_size \
            else index + 2 * self._send_size

        for i in range(start, finish):
            self._status[i] = 1
        return index, a, b, self._iter

    def set_list(self, index, sorted, num_iter):
        """recebe o índice do início da escrita,
        a lista ordenada e número da iteração"""

        # escrevemos os blocos na mesma iteração e se ainda não foi escrito
        if num_iter == self._iter and self._status[index] == 1:

            # marcamos os elementos como recebido
            for i in range(index, index + len(sorted)):
                self._status[i] = 2

            # atualizamos a lista com os elementos ordenados
            for i in range(len(sorted)):
                self._num_list[index+i] = sorted[i]

            # se todos os elementos foram ordenados, subimos de iteração,
            # dobramos o tamanho do envio e reiniciamos o status
            if min(self._status) == 2:
                self._iter += 1
                self._send_size *= 2
                self._status = [0] * self._list_size

    def is_unsorted(self):
        return self._send_size < self._list_size

    def num_iter(self):
        return self._iter
