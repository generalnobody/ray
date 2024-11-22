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


//extern internal::Work;

namespace external_scheduler {

const int PORT = 8080;
const std::string IP_ADDR = "127.0.0.1";
int socket_fd;

enum API_CODES : uint8_t{
    ADD_NODE = 0x0,
    REMOVE_NODE = 0x1,
    SCHEDULE = 0x2,
};

//TODO add thread safety?
void full_send(void* data, size_t size);
size_t full_recv(void* data, size_t max_size);

void send_resources(const absl::flat_hash_map<std::string, double>& resource_map);
scheduling::NodeID schedule(const ResourceRequest& resources);
void add_node(scheduling::NodeID node, const NodeResources& resources);
void remove_node(scheduling::NodeID node);

void init();

void die();

}//end namespace external_scheduler