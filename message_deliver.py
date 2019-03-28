import queue
import socket
import threading


class MessageDeliver(object):
    PREAMBLE = b"\x01\x00\x00\x00\x00\x00\x02"
    EOT = b"\x03\x04"

    def __init__(self, op_type="client", host=None, port=3040, listen=1024):
        if op_type not in ["client", "server"]:
            raise Exception("Error not support operation type. [%s]" % op_type)
        if op_type == "client":
            if host is None:
                raise Exception("Error it is not input.")
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((host, port))
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(("", port))
            self.sock.listen(listen)
        self.op_type = op_type
        self.send_workers = []
        self.receive_workers = []
        self.queue = queue.Queue()
        self.get_queue = queue.Queue()

    def _receiver(self, sock):
        preamble = sock.recv(7)
        if preamble == b"":
            return None
        if preamble != self.PREAMBLE:
            raise Exception("Corrupted this data "
                            "because of the invalid Preamble.")
        length_hex = sock.recv(4)
        length = int.from_bytes(length_hex, "big")
        msg = sock.recv(length)
        eot = sock.recv(2)
        if eot != self.EOT:
            raise Exception("Corrupted this data "
                            "because of the invalid EndOfText.")
        return msg

    def _pack_msg(self, msg):
        length = len(msg)
        length_raw = length.to_bytes(4, "big")
        raw = self.PREAMBLE + length_raw + msg + self.EOT
        return raw

    def _listener(self):
        while 1:
            client, cliaddr = self.sock.accept()
            ths = threading.Thread(target=self.server_sender, args=(client,))
            ths.setDaemon(True)
            self.send_workers.append(ths)
            ths.start()
            thr = threading.Thread(target=self.server_receiver, args=(client,))
            thr.setDaemon(True)
            self.send_workers.append(thr)
            thr.start()

    def server_sender(self, client):
        while 1:
            msg = self.queue.get()
            try:
                client.send(self._pack_msg(msg))
            except BrokenPipeError:
                self.queue.put(msg)
                break

    def server_receiver(self, client):
        while 1:
            msg = self._receiver(client)
            if msg is None:
                break
            self.get_queue.put(msg)

    def client_sender(self):
        while 1:
            msg = self.queue.get()
            try:
                self.sock.send(self._pack_msg(msg))
            except BrokenPipeError:
                self.queue.put(msg)
                break

    def client_receiver(self):
        while 1:
            msg = self._receiver(self.sock)
            if msg is None:
                break
            self.get_queue.put(msg)

    def put(self, msg):
        self.queue.put(msg)

    def get(self):
        msg = self.get_queue.get()
        return msg

    def start(self):
        if self.op_type == "server":
            th = threading.Thread(target=self._listener)
            th.setDaemon(True)
            th.start()
        elif self.op_type == "client":
            ths = threading.Thread(target=self.client_sender)
            ths.setDaemon(True)
            ths.start()
            thr = threading.Thread(target=self.client_receiver)
            thr.setDaemon(True)
            thr.start()
        else:
            raise Exception("Not supported operation type")
