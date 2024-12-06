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
#include <assert.h>

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


//TODO add singleton/ thread safety?
void full_send(void* data, size_t size, int socket_fd){

    ssize_t ok = write(socket_fd, &size, sizeof(size));

    if(ok != sizeof(size)){
        std::cerr << "Failed to send size in full_send" << std::endl;
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
            std::cerr << "Failed to send remaining data in full_send" << std::endl;
            close(socket_fd);
            exit(0);
        }
    }while(size > 0);
}

bool receive_ok(int socket_fd){
    size_t size = 0;

    ssize_t ok = recv(socket_fd, &size, sizeof(size), 0);

    if(ok != sizeof(size)){
        std::cerr << "Failed to receive size in receive_ok" << std::endl;
        close(socket_fd);
        exit(0);
    }
    assert(size == 1);

    uint8_t zero = 0;
    ok = recv(socket_fd, &zero, sizeof(zero), 0);
    if(ok != 1){
        std::cerr << "Failed to receive 0 in receive_ok" << std::endl;
        close(socket_fd);
        exit(0);
    }
    return zero == 0;
}

std::vector<uint8_t> resources_to_vector(const absl::flat_hash_map<std::string, double>& resource_map){//returns a vector without the size
    std::vector<uint8_t> data;
    data.push_back(SCHEDULE);//default schedule, can be changed by other fucntions.

    for(auto it = resource_map.begin(); it != resource_map.end(); it++){
        std::string name = it->first;
        double value = it->second;

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



scheduling::NodeID schedule(const ResourceRequest& resources, State state){
    std::cout << "EXTERNAL SCHEDULER: EXECUTING SCHEDULE NOW!" << std::endl;

    if(!state.initialized){
        std::cerr << "EXTERNAL SCHEDULER schedule() but not initialized" << std::endl;
        exit(1);
    }
    
    std::vector<uint8_t> data = resources_to_vector(resources.ToResourceMap());
    full_send(data.data(), data.size(), state.socket_fd);

    if(receive_ok(state.socket_fd)){
        int64_t nodeID;
        ssize_t ok = recv(state.socket_fd, &nodeID, sizeof(nodeID), 0);

        if(ok != sizeof(nodeID)){
            std::cerr << "Failed to receive nodeID in schedule" << std::endl;
            close(state.socket_fd);
            exit(0);
        }
        std::cout << "EXTERNAL SCHEDULER: FOUND GOOD NODE" << std::endl;
        return scheduling::NodeID(nodeID);
    }
    std::cout << "EXTERNAL SCHEDULER: FOUND NO NODE!" << std::endl;
    return scheduling::NodeID::Nil();
}


void add_node(scheduling::NodeID node, const NodeResources& resources, State state){//see cluster_resource_manager.cc
    std::cout << "EXTERNAL SCHEDULER: EXECUTING ADD NODE NOW. ADDING NODE: " << node.ToInt() << std::endl;
    std::vector<uint8_t> data = resources_to_vector(resources.available.GetResourceMap());
    data.at(sizeof(size_t)) = ADD_NODE;
    full_send(data.data(), data.size(), state.socket_fd);

    assert(receive_ok(state.socket_fd));
    std::cout << "EXTERNAL SCHEDULER: ADD NODE COMPLETE" << std::endl;
}

void remove_node(scheduling::NodeID node, State state){//see cluster_resource_manager.cc
    std::cout << "EXTERNAL SCHEDULER: EXECUTING REMOVE NODE NOW! REMOVING NODE:" << node.ToInt() << std::endl;

    int64_t node_ID = node.ToInt();
    std::vector<uint8_t> data;
    data.push_back(REMOVE_NODE);

    uint8_t *tmp = (uint8_t *)&node_ID;
    for(size_t i = 0; i < sizeof(node_ID); i++){
        data.push_back(tmp[i]);
    }
    full_send(data.data(), data.size(), state.socket_fd);

    assert(receive_ok(state.socket_fd));
    std::cout << "EXTERNAL SCHEDULER: REMOVE NODE COMPLETE" << std::endl;
}

struct State init(){
    std::cout << "EXTERNAL SCHEDULER: INIT NOW" << std::endl;
    struct sockaddr_in servaddr;
    const std::string config_name = "EXERNAL_SCHEDULER_CONFIG.txt";
    std::ifstream config;
    config.open(config_name, std::ios::in);

    if(!config.is_open()){
        std::cerr << "EXTERNAL SCHEDULER: failed to open config file: \"" << config_name << "\"" << std::endl;
        exit(0);
    }
    struct State result;

    config >> result.IP_ADDR;
    config >> result.PORT;

    // socket create and verification
    result.socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (result.socket_fd == -1) {
        std::cerr << "socket creation failed..." << std::endl;
        exit(0);
    }
    bzero(&servaddr, sizeof(servaddr));

    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(result.IP_ADDR.c_str());
    servaddr.sin_port = htons(result.PORT);

    // connect the client socket to server socket
    if(connect(result.socket_fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0){
        std::cerr << "connection with the server failed...\n" << std::endl;
        exit(0);
    }
    result.initialized = true;
    std::cout << "EXTERNAL SCHEDULER: CONNECTED TO SERVER, INIT COMPLETE" << std::endl;
    return result;
}

void die(struct State state){
    std::cout << "EXTERNAL SCHEDULER: DYING NOW" << std::endl;
    close(state.socket_fd);
    state.initialized = false;
    std::cout << "EXTERNAL SCHEDULER: DYING COMPLETE" << std::endl;
}

}//end namespace external_scheduler