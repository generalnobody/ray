#pragma once
#ifndef EXTERNAL_SCHEDULER_H
#define EXTERNAL_SCHEDULER_H
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
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/util/container_util.h"

#include "ray/common/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/local_resource_manager.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

#include "ray/util/container_util.h"
#include <boost/algorithm/string.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"


//extern internal::Work;

namespace external_scheduler {
    using namespace ray;

struct State{
    int PORT = 8080;
    std::string IP_ADDR = "127.0.0.1";
    int socket_fd;
    bool initialized = false;
};

enum API_CODES : uint8_t{
    ADD_NODE = 0x0,
    REMOVE_NODE = 0x1,
    SCHEDULE = 0x2,
};

//TODO add thread safety?
void full_send(void* data, size_t size, int socket_fd);
size_t full_recv(void* data, size_t max_size, int socket_fd);
bool receive_ok(int socket_fd);
void send_resources(const absl::flat_hash_map<std::string, double>& resource_map);
scheduling::NodeID schedule(const ResourceRequest& resources, struct State state);
void add_node(scheduling::NodeID node, const NodeResources& resources, struct State state);
void remove_node(scheduling::NodeID node, struct State state);

struct State init();

void die();

}//end namespace external_scheduler
#endif//define