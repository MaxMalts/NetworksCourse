from ctypes import *
import random
import socket
import struct
from threading import *
from queue import Queue
import time


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr, send_loss=0.0):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)
        self.send_loss = send_loss


    def sendto(self, data):
        if random.random() < self.send_loss:
            # simulate that packet was lost
            return len(data)

        return self.udp_socket.sendto(data, self.remote_addr)


    def recvfrom(self, n):
        msg = bytes()
        try:
            msg, addr = self.udp_socket.recvfrom(n)
        except:
            pass
        finally:
            return msg


class Header:
    header_size = 3 * sizeof(c_uint32) + sizeof(c_bool)
    
    def __init__(self, data_id: c_uint32 = 0, data_size: c_int32 = 0,
        segment_num: c_int32 = 0, ack_flag: c_bool = False):
        
        self.data_id = data_id
        self.data_size = data_size
        self.segment_num = segment_num
        self.ack_flag = ack_flag
        
        self.header_buf = struct.pack("I I I ?", data_id, data_size, segment_num, ack_flag)
        
    def set_from_buf(self, header_buf: bytes):
        self.data_id, self.data_size, self.segment_num, self.ack_flag =\
            struct.unpack("I I I ?", header_buf)
        
    def get_buffer(self):
        return self.header_buf


class MyTCPProtocol(UDPBasedProtocol):
    timeout_ms = 2
    segment_size = 50000
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #self.udp_socket.settimeout(self.timeout_ms / 1000)
        self.next_send_data_id = 0
        self.received_data = Queue()
        self.last_data_accepted = Event()
        #self.performing_socket_operation = Lock()
        
        self.recv_worker_thread = Thread(target = self.receive_worker, daemon = True)
        self.recv_worker_thread.start()


    def send(self, data: bytes):
        data_id = self.next_send_data_id
        
        #self.performing_socket_operation.acquire()
    
        self.send_split_segments(data, data_id)
        #print(self.__repr__() + " Sent data_id " + str(data_id))
        
        while(not self.last_data_accepted.wait(self.timeout_ms / 1000)):
            self.send_split_segments(data, data_id)
            #print(self.__repr__() + " Resent data_id " + str(data_id))
        
        self.next_send_data_id += 1
        self.last_data_accepted.clear()
        #print(self.__repr__() + " Received accept data_id " + str(data_id))
        
        # acception_header = Header(0, 0, 0, False)
        # while (acception_header.ack_flag == False or acception_header.data_id != data_id):
        #     received_data = self.recvfrom(Header.header_size)
        #     while (len(received_data) != Header.header_size):
        #         self.send_split_segments(data, data_id)
        #         #print(self.__repr__() + " Resent data_id " + str(data_id))
                
        #         received_data = self.recvfrom(Header.header_size)
            
        #     acception_header.set_from_buf(received_data)
        
        # #print(self.__repr__() + " Received accept data_id " + str(data_id))
        # #self.performing_socket_operation.release()
        # time.sleep(0.01)
        return len(data)

 
    def recv(self, n: int):
        data = self.received_data.get()
        return data[:min(n, len(data))]
    
    
    def send_split_segments(self, data: bytes, data_id: c_uint32):
        data_size = len(data)
        
        segment_num = 0
        cur_sent = 0
        while (cur_sent < data_size):
            header_buf = Header(data_id, data_size, segment_num, False).get_buffer()
            
            last_ind_to_send = min(cur_sent + (self.segment_size - Header.header_size), data_size)
            segment = header_buf + data[cur_sent:last_ind_to_send]

            bytes_sent = self.sendto(segment)
            assert bytes_sent == len(segment), "Wrong amount of bytes sent."
            
            segment_num += 1
            cur_sent = last_ind_to_send
    
    
    def handle_ack_income(self, header: Header):
        assert header.ack_flag == True
        #print(self.__repr__() + " Received accept in handler data_id " + str(header.data_id))
        assert header.data_id == self.next_send_data_id or\
            header.data_id == self.next_send_data_id - 1
        
        if (header.data_id == self.next_send_data_id):
            self.last_data_accepted.set()
    
    
    def process_incoming_data(self):
        header = None
        while (header == None or header.segment_num != 0):
            segment = bytes()
            #self.performing_socket_operation.acquire()
            while (len(segment) < Header.header_size):
                #self.performing_socket_operation.release()
                #time.sleep(0.00000000001)  # as there is no yield in python and sleep(0) makes nothing >:(
                #self.performing_socket_operation.acquire()
                segment = self.recvfrom(self.segment_size)
            
            header = Header()
            header.set_from_buf(segment[:Header.header_size])
        
        if (header.ack_flag == True):
            self.handle_ack_income(header)
            return None, None
        
        data = bytearray(header.data_size)
        data_size = header.data_size
        data_id = header.data_id
        
        segment_num = 0
        cur_size = 0
        while (data_size - cur_size > self.segment_size - Header.header_size):
            data[cur_size:] = segment[Header.header_size:]
            cur_size += self.segment_size - Header.header_size
            segment_num += 1
            
            header = Header(0, 0, 0, True)
            while (header.ack_flag == True or header.data_id != data_id):
                need_to_recv = min(data_size - cur_size + Header.header_size, self.segment_size)
                segment = self.recvfrom(need_to_recv)
                while (len(segment) != need_to_recv):
                    segment = self.recvfrom(need_to_recv)
                
                header.set_from_buf(segment[:Header.header_size])
            
            if (header.segment_num != segment_num):
                return None, None
        
        data[cur_size:] = segment[Header.header_size:]
        assert len(data) == data_size, "Data sizes do not match."
        
        return data, data_id
    
    
    def receive_worker(self):
        next_data_id = 0
        while True:
            data = None
            data_id = 0
            while (data == None):
                data, data_id = self.process_incoming_data()
                    
            
            assert data_id == next_data_id or data_id == next_data_id - 1
            
            #print(self.__repr__() + " Before accepted data_id " + str(data_id))
            while (self.sendto(Header(data_id, 0, 0, True).get_buffer()) != Header.header_size):
                pass
            #print(self.__repr__() + " Accepted data_id " + str(data_id))
            
            #self.performing_socket_operation.release()
            
            if (data_id == next_data_id):
                self.received_data.put(data)
                next_data_id += 1