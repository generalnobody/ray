import socket
import selectors
import struct
import argparse
import time
from enum import IntEnum
from abc import ABC

# Resources: [ascii resource string (ends in 0x0)][8 bytes resource value]; repeats...
class API_CODES(IntEnum):
    ADD_NODE = 0    # Req: [8 bytes msg len]0x0[8 bytes nodeID][resources];     Res: [8 bytes msg len]0x0
    REMOVE_NODE = 1 # Req: [8 bytes msg len]0x1[8 bytes nodeID];                Res: [8 bytes msg len]0x0
    SCHEDULE = 2    # Req: [8 bytes msg len]0x2[8 bytes taskID][resources];     Res: [8 bytes msg len]0x0[8 bytes nodeID] (if fail, instead: [8 bytes msg len]0x1)

class BaseSchedulerClass(ABC):
    def __init__(self, id, resources):
        self.id = id
        self.resources = resources

class Task(BaseSchedulerClass):
    def __init__(self, id, resources):
        super().__init__(id, resources)

class Node(BaseSchedulerClass):
    tasks = [] # List [Task]
    def __init__(self, id, resources):
        super().__init__(id, resources)

nodes = {} # Dict: Key: client_sock; Value: List [Node]
next_task_id = 0

parser = argparse.ArgumentParser(description="External Ray Scheduler Server")
parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='Host IP address (default: 127.0.0.1)')
parser.add_argument('-p', '--port', type=int, default=44444, help='Host Port (default: 44444)')

args = parser.parse_args()

sel = selectors.DefaultSelector()

# Decode the api resources format into a dict
def decode_api_resources(bytestr):
    resources = {}
    while b'\x00' in bytestr:
        key, bytestr = bytestr.split(b'\x00', 1)
        val = bytestr[:8]
        bytestr = bytestr[8:]
        resources[key] = struct.unpack('<d', val)[0]
    return resources

# Returns a list of possible nodes depending on available total resources
def find_task_possible_nodes(client_sock, resources):
    possible_nodes = []
    for node in nodes[client_sock]:
        available = True
        for key, val in resources.items():
            if not key in node.resources or node.resources[key] < val:
                available = False
                break
        if available:
            possible_nodes.append(node)
    return possible_nodes

# Finds node with shortest queue that matches task's resource requirements
def find_node_shortest_queue(possible_nodes):
    optimal_node = possible_nodes[0]
    shortest_queue_len = len(optimal_node.tasks)
    for node in possible_nodes[1:]:
        queue_len = len(node.tasks)
        if queue_len < shortest_queue_len:
            optimal_node = node
            shortest_queue_len = queue_len
    return optimal_node

# Add a new node
def add_node(client_sock, message):
    nodeID = int.from_bytes(message[:8], byteorder='little', signed=True)
    resources = decode_api_resources(message[8:])
    nodes[client_sock].append(Node(nodeID, resources))
    return b'\x00'

# Remove a node
def remove_node(client_sock, message):
    nodeID = int.from_bytes(message[:8], byteorder='little', signed=True)
    for node in nodes[client_sock]:
        if node.id == nodeID:
            nodes[client_sock].remove(node)
    return b'\x00'

# Schedule a new task
def schedule_task(client_sock, message):
    global next_task_id

    resources = decode_api_resources(message)
    possible_nodes = find_task_possible_nodes(client_sock, resources)
    if len(possible_nodes) == 0:
        return b'\x01'
    
    optimal_node = find_node_shortest_queue(possible_nodes)
    optimal_node.tasks.append(Task(next_task_id, resources))
    next_task_id += 1
    return b'\x00' + optimal_node.id.to_bytes(8, byteorder='little', signed=True)

def handle_message(client_sock, message):
    api_code = message[0]
    message = message[1:]
    if api_code == API_CODES.ADD_NODE:
        return add_node(client_sock, message)
    elif api_code == API_CODES.REMOVE_NODE:
        return remove_node(client_sock, message)
    elif api_code == API_CODES.SCHEDULE:
        return schedule_task(client_sock, message)
    else:
        return b'\x01'


def accept_connection(server_sock):
    # Accept a new connection
    client_sock, addr = server_sock.accept()
    print(f"Client connected: {addr}")
    client_sock.setblocking(False)
    sel.register(client_sock, selectors.EVENT_READ, read_client)
    nodes[client_sock] = []

def read_client(client_sock):
    # Read data from client socket
    try:
        # Get message length
        msg = b''
        while len(msg) < 8:
            tmp = client_sock.recv(8 - len(msg))
            if not tmp:
                close_connection(client_sock)
                return
            msg += tmp
        
        # Get full message
        msg_len = int.from_bytes(msg[:8], byteorder='little', signed=True)
        msg = b''
        while len(msg) < msg_len:
            try:
                tmp = client_sock.recv(msg_len - len(msg))
            except (BlockingIOError):
                print("...")
                time.sleep(1)
                continue

            if not tmp:
                close_connection(client_sock)
                return
            msg += tmp    

        print(f"[{client_sock.fileno()}] incoming message: {msg}")
        # Process message
        ret_message = handle_message(client_sock, msg)
        print(f"[{client_sock.fileno()}] response message: {ret_message}")

        # Send message back
        ret_msg_len = len(ret_message)
        print(f"[{client_sock.fileno()}] replying with message: '{ret_message.hex()}'; length: '{ret_msg_len}'")
        client_sock.send(ret_msg_len.to_bytes(8, byteorder='little') + ret_message)        
    except ConnectionResetError:
        close_connection(client_sock)

def close_connection(client_sock):
    # Close client connection
    print("Closing connection.")
    sel.unregister(client_sock)
    del nodes[client_sock]
    client_sock.close()

def start_server(host, port):
    # Start TCP server
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind((host, port))
    server_sock.listen()
    server_sock.setblocking(False)

    # Use selector to allow for multiple sockets at the same time
    sel.register(server_sock, selectors.EVENT_READ, accept_connection)
    print(f"Server started on {host}:{port}")

    try:
        while True:
            for key, mask in sel.select(timeout=None):
                cb = key.data
                cb(key.fileobj)
    except KeyboardInterrupt:
        print("Shutting down the server...")
    finally:
        sel.close()
        server_sock.close()

def main():
    start_server(args.address, args.port)

if __name__ == "__main__":
    main()
