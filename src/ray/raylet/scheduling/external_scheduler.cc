#include <iostream>
#include <string>
#include <map>

#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#include <assert.h>

#include "ray/common/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/local_resource_manager.h"
#include "ray/common/scheduling/cluster_resource_manager.h"

#include "external_scheduler.h"

namespace external_scheduler {

const int PORT 8080;
const std::string IP_ADDR = "127.0.0.1";
int socket_fd;
bool initialized = false;


enum API_CODES : uint8_t{
    ADD_NODE = 0x0,
    REMOVE_NODE = 0x1,
    SCHEDULE = 0x2,
};

//TODO add singleton/ thread safety?
void full_send(void* data, size_t size){
    if(!initialized){
        init();
    }
    do{
       ok = write(socket_fd, data, size);

        if(ok > 0){
            data += ok;
            size -= ok;
        }
        else{
            std::cerr << "Failed to send size in full_send" << std::endl;
            close(socket_fd);
            exit(0);
        }
    }while(size > 0);
}

size_t full_recv(void* data, size_t max_size){
    if(!initialized){
        init();
    }
    if(ok != sizeof(size)){
        close(socket_fd);
        exit(0);
    }
    ok = recv(socket_fd, data, size, MSG_WAITALL);
    
    if(ok != size){
        close(socket_fd);
        exit(0);
    }
    return size;
}

void send_resources(const absl::flat_hash_map<std::string, double>& resource_map){//does not end the message

    for(std::pair<std::string, double> resource[name, value] : resource_map){
        full_send(name.c_str(), name.length() + 1);//also send the NULL terminator
        full_send(&value, sizeof(double));
    }
}


scheduling::NodeID schedule(const ResourceRequest& resources){
    std::cout << "EXTERNAL SCHEDULER: EXECUTING SCHEDULE NOW!" << std::endl;
    API_CODES code = SCHEDULE;
    full_send(&code, 1);
    send_resources(resources.ToResourceMap());

    char new_line = '\n';
    full_send(&new_line, 1);//to signal end of message

    uint8_t ok = 0;
    full_recv(&ok, 1);

    if(ok == 0){
        int64_t nodeID;
        full_recv(&nodeID, sizeof(int64_t));
        std::cout << "EXTERNAL SCHEDULER: FOUND GOOD NODE" << std::endl;
        return scheduling::NodeID(nodeID);
    }
    else{
        std::cout << "EXTERNAL SCHEDULER: FOUND NO NODE!" << std::endl;
        return scheduling::NodeID::Nil();
    }
}


void add_node(scheduling::NodeID node, const NodeResources& resources){//see cluster_resource_manager.cc
    std::cout << "EXTERNAL SCHEDULER: EXECUTING ADD NODE NOW. ADDING NODE: " << node.ToInt() << std::endl;
    uint8_t code = ADD_NODE;
    full_send(&code, 1);
    int64_t id = node.ToInt();
    full_send(&id, sizeof(int64_t));
    NodeResourceInstanceSet instance_set = NodeResourceInstanceSet(resources);//resource_instance_set.h
    NodeResourceSet resource_set = instance_set.ToNodeResourceSet();//resource_set.h
    send_resources(resource_set.GetResourceMap(););
    
    char new_line = '\n';
    full_send(&new_line, 1);//to signal end of message

    uint8_t ok = 0;
    full_recv(&ok, 1);
    assert(ok == 0);
    std::cout << "EXTERNAL SCHEDULER: ADD NODE COMPLETE" << std::endl;
}

void remove_node(scheduling::NodeID node){//see cluster_resource_manager.cc
    std::cout << "EXTERNAL SCHEDULER: EXECUTING REMOVE NODE NOW! REMOVING NODE:" << node.ToInt() << std::endl;
    API_CODES code = REMOVE_NODE;
    full_send(&code, 1);
    uint64_t node_ID = node.ToInt();
    full_send(&node_ID, sizeof(uint64_t));
    std::cout << "EXTERNAL SCHEDULER: REMOVE NODE COMPLETE" << std::endl;
}

void init(){
    std::cout << "EXTERNAL SCHEDULER: INIT NOW" << std::endl;
    struct sockaddr_in servaddr;

    // socket create and verification
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        std::cerr << "socket creation failed..." << std::endl;
        exit(0);
    }
    else
    bzero(&servaddr, sizeof(servaddr));

    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(IP_ADDR);
    servaddr.sin_port = htons(PORT);

    // connect the client socket to server socket
    if(connect(socket_fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0){
        std::cerr << "connection with the server failed...\n" << std::endl;
        exit(0);
    }
    initialized = true;
    std::cout << "EXTERNAL SCHEDULER: CONNECTED TO SERVER, INIT COMPLETE" << std::endl;
}

void die(){
    std::cout << "EXTERNAL SCHEDULER: DYING NOW" << std::endl;
    close(socket_fd);
    initialized = false;
    std::cout << "EXTERNAL SCHEDULER: DYING COMPLETE" << std::endl;
}

}//end namespace external_scheduler