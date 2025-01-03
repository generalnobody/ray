import socket
import selectors
import struct
from enum import Enum, IntEnum
import time

class API_CODES(IntEnum):
    ADD_NODE = 0
    REMOVE_NODE = 1
    SCHEDULE = 2

HOST = '127.0.0.1'
PORT = 44444

sel = selectors.DefaultSelector()
# Should probably separate nodes by which client added them? Something to check out later
node_info = {} #Key: connection, Value: pair of (nodes, task queues) 
nodes = {} # Key: nodeID; Value: Dict {Key: ascii resource key; Value: resource value}
# Currently just increments task_counter and uses that as taskID. Therefore, cannot handle removing tasks from the queue, because of which some queues can get increasingly long while others are empty
task_queue = {} # Key: nodeID; Value: List{taskID} 
task_counter = 0

# Decode the way the api resources format into a dict
def decode_api_resources(bytestr):
    resources = {}
    while b'\x00' in bytestr:
        key, bytestr = bytestr.split(b'\x00', 1)
        val = bytestr[:8]
        bytestr = bytestr[8:]
        resources[key] = struct.unpack('<d', val)[0]
    print("api resources:", resources)
    return resources

# Finds node with shortest queue that matches task's resource requirements
def find_optimal_node(client_sock, resources):
    (nodes, task_queue) = node_info[client_sock.fileno]
    possible_nodes = []
    available = True
    for nodeID, avail_resources in nodes.items():
        for key, val in resources.items():
            if not key in avail_resources or avail_resources[key] < val:
                available = False
                break
        if available:
            possible_nodes.append(nodeID)
        else:
            available = True

    if len(possible_nodes) == 0:
        return 1, 0

    optimal_node = possible_nodes[0]
    shortest_queue_len = len(task_queue[optimal_node])
    for nodeID in possible_nodes[1:]:
        queue_len = len(task_queue[nodeID])
        if queue_len < shortest_queue_len:
            optimal_node = nodeID
            shortest_queue_len = queue_len
    return 0, optimal_node

def handle_message(client_sock, message):
    (nodes, task_queue) = node_info[client_sock.fileno]
    global task_counter
    api_code = message[0]
    message = message[1:]
    if api_code == API_CODES.ADD_NODE:
        
        nodeID = int.from_bytes(message[:8], byteorder='little', signed=True)
        print("add node message received, node id:", message[:8].hex())
        resources = decode_api_resources(message[8:])
        nodes[nodeID] = resources
        task_queue[nodeID] = []
        return b'\x00'
    elif api_code == API_CODES.REMOVE_NODE:
        nodeID = int.from_bytes(message[:8], byteorder='little', signed=True)
        print("remove node message received, node id:", message[:8].hex())
        
        if not nodeID in nodes:
            return b'\x00'
        del task_queue[nodeID]
        return b'\x00'
    elif api_code == API_CODES.SCHEDULE:
        print("schedule message received")
        resources = decode_api_resources(message)
        code, optimal_node = find_optimal_node(client_sock, resources)
        if code == 1:
            return b'\x01'
        task_queue[optimal_node].append(task_counter)
        task_counter+=1
        return b'\x00' + optimal_node.to_bytes(8, byteorder='little', signed=True)
    else:
        return b'\x01'


def accept_connection(server_sock):
    # Accept a new connection
    client_sock, addr = server_sock.accept()
    print(f"Client connected: {addr}")
    client_sock.setblocking(False)
    sel.register(client_sock, selectors.EVENT_READ, read_client)
    node_info[client_sock.fileno] = ({}, {})

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
                print("...");
                time.sleep(1)
                continue
            
            if not tmp:
                close_connection(client_sock)
                return
            msg += tmp    

        # Process message
        print("input message:", msg)
        ret_message = handle_message(client_sock, msg)
        print("response message:", ret_message)
        # Send message back
        ret_msg_len = len(ret_message)
        print("replying with message:", ret_message.hex(), "length: ", ret_msg_len)
        print("length again:", ret_msg_len.to_bytes(8, byteorder='little').hex())
        client_sock.send(ret_msg_len.to_bytes(8, byteorder='little') + ret_message)        
    except ConnectionResetError:
        close_connection(client_sock)

def close_connection(client_sock):
    # Close client connection
    print("Closing connection.")
    sel.unregister(client_sock)
    client_sock.close()
    del node_info[client_sock.fileno]

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

if __name__ == "__main__":
    start_server(HOST, PORT)
