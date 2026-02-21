#include <memory>
#include <remus/remus.h>
#include <vector>
#include "cloudlab.h"

struct SharedObject {
  uint64_t value[1024];
}

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
    // create a context for each Compute Thread
    std::vector<std::shared_ptr<remus::ComputeThread>> compute_threads; 
    for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
      compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
    }

  
  // create the shared object and initialize it
  // the process is still sequential
  // the main thread on CN0 will create the shared object using one of the 
  //   available ComputeThread objects
  if (id == c0) {
    // when you call allocate, Remus will use --alloc-pol flag to determine
    //   which MN the current Compute Thread should allocate from
    auto ptr = compute_threads[0]->allocate<SharedObject>();
    for (unsigned i = 0; i < 1024; ++i) {
      // since the memory isn't local, need to initialize it explicitly
      compute_threads[0]->Write<uint64_t>(
		    remus::rdma_ptr<uint64_t>(ptr.raw() +
			                      offsetof(SharedObject, values[0])), 
		    (uint64_t)0);  
    }
    // make the SharedObject visible through the global root pointer, 
    //   which is at MN0, Segment 0
    compute_threads[0]->set_root(ptr);
  }  
}
