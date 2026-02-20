#include <memory>
#include <remus/remus.h>
#include <vector>
#include "cloudlab.h"

int main(int argc, char **argv) {
  remus::INIT();

  // configure and parse the arguments
  auto args = std::make_shared<remus::ArgMap>();
  args->import(remus::ARGS);
  args->parse(argc, argv);
  if (args->bget(remus::HELP)) {
    args->usage();
    return 0;
  }
  args->report_config();

  // extract the args for determining names and roles
  uint64_t id = args->uget(remus::NODE_ID);
  uint64_t m0 = args->uget(remus::FIRST_MN_ID);
  uint64_t mn = args->uget(remus::LAST_MN_ID);
  uint64_t c0 = args->uget(remus::FIRST_CN_ID);
  uint64_t cn = args->uget(remus::LAST_CN_ID);

  // using id_to_dns_name from cloudlab.h and the m0 and mn arguments, 
  // 	can compute the DNS names of the machines that will serve as memory nodes
  // prepare network information about this machine and about memnodes
  std::vector<remus::MachineInfo> memnodes; 
  for (uint64_t i = m0; i < mn; ++i) {
    memnodes.emplace_back(i, id_to_dns_name(i));
  }

  // every node (whether MN or CN) will need to know its specific 
  // 	numerical identifier
  // compute the name of this machine
  remus::MachineInfo self(id, id_to_dns_name(id));

  // configure as a Memory Node? 
  std::unique_ptr<remus::MemoryNode> memory_node;
  if (id >= m0 && id <= mn) {
    // make the pools, await connections
    memory_node.reset(new remus::MemoryNode(self, args));
  }

  // configure as a Compute Node?
  std::shared_ptr<remus::ComputeNode> compute_node;
  if (id >= 0 && id <= cn) {
    compute_node.reset(new remus::ComputeNode(self, args));
    // if this CN is also a MN, then we need to pass the rkeys to the local
    // 	  MN  -- no harm in doing them first
    if (memory_node.get() != nullptr) {
      compute_node->connect_local(memnodes, memory_node->get_local_rkeys());
    }
    compute_node->connect_remote(memnodes);
  }

  // not a good idea to keep threads running!!
  // when all CNs have finished connecting to the MN, immediately instruct
  // 	the MN to stop its listening thread
  // reclaim threads when all connections have been made
  if (memory_node) {
    memory_node->init_done();
  }

  // create Compute Threads so they can signal to the MNs that they've completed
  if (id >= c0 && id <= cn) {
    std::vector<std::shared_ptr<remus::ComputeThread>> compute_threads; 
    for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
      compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
    }
  }  
}
