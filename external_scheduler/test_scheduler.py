import socket

def resources_to_bytestring(resources):
    bytestr = b''
    for key, value in resources.items():
        bytestr += key.encode('ascii') + b'\x00'
        bytestr += value.to_bytes(8, byteorder='little', signed=True)
    return bytestr

node1ID = 50
node1Resources = {
    "cpu": 5,
    "gpu": 10
}
node2ID = 60
node2Resources = {
    "cpu": 10,
    "gpu": 20
}
nodeBadID = 70 # Test removing bad id
add_node1 = b'\x00' + node1ID.to_bytes(8, byteorder='little', signed=True) + resources_to_bytestring(node1Resources)
add_node2 = b'\x00' + node2ID.to_bytes(8, byteorder='little', signed=True) + resources_to_bytestring(node2Resources)

task1 = {
    "cpu": 7,
    "gpu": 5
}
task2 = {
    "cpu": 2,
    "gpu": 10
}
task3 = { # None available
    "cpu": 20,
}
schedule_task1 = b'\x02' + resources_to_bytestring(task1)
schedule_task2 = b'\x02' + resources_to_bytestring(task2)
schedule_task3 = b'\x02' + resources_to_bytestring(task3)

remove_nodeBad = b'\x01' + nodeBadID.to_bytes(8, byteorder='little', signed=True)
remove_node1 = b'\x01' + node1ID.to_bytes(8, byteorder='little', signed=True)
remove_node2 = b'\x01' + node2ID.to_bytes(8, byteorder='little', signed=True)

############################################################################################

HOST = '127.0.0.1'
PORT = 44444

client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_sock.connect((HOST, PORT))

def send_message(sock, msg):
    msg_len = len(msg)
    sock.send(msg_len.to_bytes(8, byteorder='little', signed=True) + msg)

def recv_message(sock):
    msg = b''
    while len(msg) < 8:
        msg += sock.recv(8 - len(msg))
    msg_len = int.from_bytes(msg[:8], byteorder='little', signed=True)
    msg = b''
    while len(msg) < msg_len:
        msg += sock.recv(msg_len - len(msg))
    return msg

print("Adding node1")
send_message(client_sock, add_node1)
buff = recv_message(client_sock)
print(buff)

print("Adding node2")
send_message(client_sock, add_node2)
buff = recv_message(client_sock)
print(buff)


print("Scheduling task1")
send_message(client_sock, schedule_task1)
buff = recv_message(client_sock)
print(buff[0])
print(int.from_bytes(buff[1:9], byteorder='little', signed=True))
print(buff[9:])

print("Scheduling task2")
send_message(client_sock, schedule_task2)
buff = recv_message(client_sock)
print(buff[0])
print(int.from_bytes(buff[1:9], byteorder='little', signed=True))
print(buff[9:])

print("Scheduling bad task3")
send_message(client_sock, schedule_task3)
buff = recv_message(client_sock)
print(buff)


print("Removing bad node")
send_message(client_sock, remove_nodeBad)
buff = recv_message(client_sock)
print(buff)

print("Removing node1")
send_message(client_sock, remove_node1)
buff = recv_message(client_sock)
print(buff)

print("Removing node2")
send_message(client_sock, remove_node2)
buff = recv_message(client_sock)
print(buff)
