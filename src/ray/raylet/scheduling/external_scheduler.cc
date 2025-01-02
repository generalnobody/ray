#include <iostream>
#include <string>
#include <map>
#include <fstream>

#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include "ray/common/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/local_resource_manager.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

#include "ray/util/container_util.h"

#include <boost/algorithm/string.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"

#include "external_scheduler.h"

namespace external_scheduler {
    using namespace ray;
    using namespace ::ray::raylet_scheduling_policy;//for scheduling::NodeID


void full_send(void* data, size_t size, int socket_fd){

    ssize_t ok = write(socket_fd, &size, sizeof(size));

    if(ok != sizeof(size)){//TODO do the reads/write in a while loop.
        //std::cerr << "Failed to send size in full_send"ted f << std::endl;
        close(socket_fd);
        exit(0);
    }

    do{
       ok = write(socket_fd, data, size);

        if(ok > 0){
            uint64_t tmp = (uint64_t)data;
            tmp += ok;
            data = (void*)tmp;
            size -= ok;
        }
        else{
            //std::cerr << "Failed to send remaining data in full_send" << std::endl;
            close(socket_fd);
            exit(0);
        }
    }while(size > 0);
}

std::vector<uint8_t> full_recv(int socket_fd){
    std::vector<uint8_t> result;
    size_t size = 0;

    ssize_t ok = recv(socket_fd, &size, sizeof(size), 0);

    if(ok != sizeof(size)){
        if(ok < 0){
            //std::cerr << "recv returned: " << ok << std::endl;
        }
        if(ok == 0){
            //std::cerr << "recv returned 0. connection closed?" << std::endl;
        }

        //std::cerr << "in full recv Failed to receive size in receive_ok, recv returned: " << ok << std::endl;
        close(socket_fd);
        exit(0);
    }
    //std::cerr << "receive size is: " << size << std::endl;
    result.reserve(size);

    while(result.size() < size){
        uint8_t byte = 0;
        ok = recv(socket_fd, &byte, 1, 0);
        if(ok < 0){
            //std::cerr << "recv returned: " << ok << std::endl;
        }
        if(ok == 0){
            //std::cerr << "recv returned 0. connection closed?" << std::endl;
        }
        result.push_back(byte);
    }
    //std::cerr << "EXTERNAL SCHEDULER incoming:" << std::endl;

    return result;
}



bool receive_ok(int socket_fd){
    size_t size = 0;

    ssize_t ok = recv(socket_fd, &size, sizeof(size), 0);

    if(ok != sizeof(size)){
        if(ok < 0){
            //std::cerr << "recv returned: " << ok << std::endl;
        }
        if(ok == 0){
            //std::cerr << "reNilcv returned 0. connection closed?" << std::endl;
        }

        //std::cerr << "Failed to receive size in receive_ok, recv returned: " << ok << std::endl;
        close(socket_fd);
        exit(0);
    }
    //std::cerr << "EXTERNAL SCHEDULER in receive_ok, size of msg received: " << size << std::endl;
    if(size != 1){
        //std::cerr << "EXTERNAL SCHEDULER: expected to receive 1 byte, not: " << size << std::endl;
        exit(0); 
    }

    uint8_t zero = 0;
    ok = recv(socket_fd, &zero, sizeof(zero), 0);
    if(ok != 1){
        //std::cerr << "Failed to receive 0 in receive_ok" << std::endl;
        close(socket_fd);
        exit(0);
    }
    if(zero == 0){
        //std::cerr << "EXTERNAL SCHEDULER. receive_ok is good" << std::endl;
    }
    else{
        //std::cerr << "EXTERNAL SCHEDULER. receive_ok is bad" << std::endl;
    }
    return zero == 0;
}

std::vector<uint8_t> resources_to_vector(const absl::flat_hash_map<std::string, double>& resource_map, API_CODES msg_type, scheduling::NodeID id = scheduling::NodeID::Nil()){//returns a vector without the size
    std::vector<uint8_t> data;
    data.push_back(msg_type);//default schedule, can be changed by other fucntions.

    if(msg_type == ADD_NODE){
        int64_t node_ID = id.ToInt();

        uint8_t *tmp = (uint8_t *)&node_ID;
        for(size_t i = 0; i < sizeof(node_ID); i++){
            data.push_back(tmp[i]);
        }
    }
    //std::cerr << "EXTERNAL SCHEDULER these are the resources: ";

    for(auto it = resource_map.begin(); it != resource_map.end(); it++){
        std::string name = it->first;
        double value = it->second;

        //std::cerr << name  << " " << value << ", ";

        for(char c : name){
            data.push_back((uint8_t)c);
        }
        data.push_back(0);//null terminator
        uint8_t *tmp = (uint8_t *)&value;
        for(size_t i = 0; i < sizeof(double); i++){
            data.push_back(tmp[i]);
        }
    }
    return data;
}



scheduling::NodeID schedule(const ResourceRequest& resources, struct State state){
    uint8_t byte = 10;
    /*if(recv(state.socket_fd, &byte, 1, MSG_PEEK) != 0){
        //std::cerr << "EXTERNAL SCHEDULER: unhandled reply before schedule" << std::endl;
        exit(0);
    }*/
    //std::cerr << "EXTERNAL SCHEDULER: EXECUTING SCHEDULE NOW 123!" << std::endl;

    if(!state.initialized){
        //std::cerr << "EXTERNAL SCHEDULER schedule() but not initialized" << std::endl;
        exit(1);
    }
    byte = 10;
    std::vector<uint8_t> data = resources_to_vector(resources.ToResourceMap(), SCHEDULE);
    data.front() = byte;
    data.front() = SCHEDULE;
    full_send(data.data(), data.size(), state.socket_fd);
    std::vector<uint8_t> reply = full_recv(state.socket_fd);

    if(reply.at(0) == 0){
        int64_t* nodeID = (int64_t*)&reply.at(1);
        //*nodeID = convert_endian(*nodeID);
        scheduling::NodeID result(*nodeID);

        if(*nodeID != scheduling::NodeID(*nodeID).ToInt()){
            //std::cerr << "EXTERNAL SCHEDULER: nodeID creation fialed" << std::endl;
            exit(0);
        }

        //std::cerr << "EXTERNAL SCHEDULER: FOUND GOOD NODE: " << std::hex << *nodeID << std::endl;
        //std::cerr << "EXTERNAL SCHEDULER: GOOD NODE BINARY:" << result.Binary() << std::endl;
        return result;
    }
    //std::cerr << "EXTERNAL SCHEDULER: FOUND NO NODE!" << std::endl;
    return scheduling::NodeID::Nil();
}


void add_node(scheduling::NodeID node, const NodeResources& resources, struct State state){//see cluster_resource_manager.cc
    uint8_t byte = 5;
    /*
    if(recv(state.socket_fd, &byte, 1, MSG_PEEK) != 0){
        //std::cerr << "EXTERNAL SCHEDULER: unhandled reply before add_node" << std::endl;
        exit(0);
    }*/
    if(byte == 4){ 
        //std::cerr << "a" << std::endl;
    }

    //std::cerr << "EXTERNAL SCHEDULER: EXECUTING ADD NODE NOW. ADDING NODE: " << std::hex << node.ToInt() << std::endl;
    std::vector<uint8_t> data = resources_to_vector(resources.available.GetResourceMap(), ADD_NODE, node);
    full_send(data.data(), data.size(), state.socket_fd);
    
    if(!receive_ok(state.socket_fd)){
        //std::cerr << "EXTERNAL SCHEDULER: server replied not OK for add_node" << std::endl;
        exit(0);
    }
    //std::cerr << "EXTERNAL SCHEDULER: ADD NODE COMPLETE" << std::endl;
}

void remove_node(scheduling::NodeID node, struct State state){//see cluster_resource_manager.cc
    uint8_t byte = 5;
    
    /*if(recv(state.socket_fd, &byte, 1, MSG_PEEK) != 0){
        //std::cerr << "EXTERNAL SCHEDULER: unhandled reply before remove_node" << std::endl;
        exit(0);
    }*/
    //std::cerr << "EXTERNAL SCHEDULER: EXECUTING REMOVE NODE NOW! REMOVING NODE:" << std::hex << node.ToInt() << std::endl;

    if(byte == 4){ 
        //std::cerr << "a" << std::endl;
    }
    byte = 10;
    int64_t node_ID = node.ToInt();
    std::vector<uint8_t> data;
    data.push_back(REMOVE_NODE);

    uint8_t *tmp = (uint8_t *)&node_ID;
    for(size_t i = 0; i < sizeof(node_ID); i++){
        data.push_back(tmp[i]);
    }
    full_send(data.data(), data.size(), state.socket_fd);
    
    if(!receive_ok(state.socket_fd)){
        //std::cerr << "EXTERNAL SCHEDULER: server replied not OK for remove_node" << std::endl;
        exit(0);
    }
    //std::cerr << "EXTERNAL SCHEDULER: REMOVE NODE COMPLETE" << std::endl;
}

struct State init(){

    //std::cerr << "EXTERNAL SCHEDULER: INIT NOW" << std::endl;
    struct sockaddr_in servaddr;
    const std::string config_name = "EXTERNAL_SCHEDULER_CONFIG.txt";
    std::ifstream config;
    config.open(config_name, std::ios::in);

    if(!config.is_open()){
        //std::cerr << "EXTERNAL SCHEDULER: failed to open config file: \"" << config_name << "\"" << std::endl;
        exit(0);
    }
    struct State result;

    config >> result.IP_ADDR;
    config >> result.PORT;

    // socket create and verification
    result.socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (result.socket_fd == -1) {
        //std::cerr << "socket creation failed..." << std::endl;
        exit(0);
    }
    bzero(&servaddr, sizeof(servaddr));

    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(result.IP_ADDR.c_str());
    servaddr.sin_port = htons(result.PORT);

    // connect the client socket to server socket
    if(connect(result.socket_fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0){
        //std::cerr << "connection with the server failed...\n" << std::endl;
        exit(0);
    }
    result.initialized = true;
    //std::cerr << "EXTERNAL SCHEDULER: INIT COMPLETE" << std::endl;
    return result;
}

void die(struct State state){
    //std::cerr << "EXTERNAL SCHEDULER: DYING NOW" << std::endl;
    close(state.socket_fd);
    state.initialized = false;
    //std::cerr << "EXTERNAL SCHEDULER: DYING COMPLETE" << std::endl;
}

}//end namespace external_scheduler