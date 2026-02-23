#include <memory>
#include <remus/remus.h>
#include <vector>
#include "cloudlab.h"

struct SharedObject {
  uint64_t values[1024];
};

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
  uint64_t threads = args->uget(remus::CN_THREADS);

  // 

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
  std::vector<std::shared_ptr<remus::ComputeThread>> compute_threads; 
  if (id >= c0 && id <= cn) {
    // create a context for each Compute Thread
    for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
      compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
    }
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
			                      offsetof(SharedObject, values[i])), 
		    (uint64_t)0);  
    }
    // make the SharedObject visible through the global root pointer, 
    //   which is at MN0, Segment 0
    compute_threads[0]->set_root(ptr);

  }

    // now we've got a properly initialized object and it's globally accessible through the root pointer 
    

    // !! start some threads !! //
    
    // barriers will need to know the total number of threads across all machines
    uint64_t total_threads = (cn - c0 + 1) * args->uget(remus::CN_THREADS);

    // create threads on this Compute Node
    //   the main thread will stop doing work now 
    std::vector<std::thread> local_threads; 
    for (uint64_t i = 0; i < threads; ++i) {
      uint64_t local_id = i; 
      uint64_t global_thread_id = id * threads + i; 
      auto t = compute_threads[i]; 
      local_threads.push_back(std::thread([t, global_thread_id, total_threads, local_id, id]() {
        // all threads, on all machines, synchronize here
        // ensures that the initialization is done and the root is updated before
        //   any machine passes the next line 
        t->arrive_control_barrier(total_threads);
        auto root = t->get_root<SharedObject>();
        
          // now, reading and writing of the SharedObject:

        // read from a unique location in SharedObject, ensure it's 0 
        auto my_loc = remus::rdma_ptr<uint64_t>(
          root.raw() + offsetof(SharedObject, values[global_thread_id + 1]));
        uint64_t my_val = t->Read<uint64_t>(my_loc);
        if (my_val != 0) {
          REMUS_FATAL("Thread {}: Read observed {}", global_thread_id, my_val);
        }

        // write a unique value to that location
        t->Write<uint64_t>(my_loc, global_thread_id); 

        // use a CompareAndSwap to increment it by 1
        bool success = 
            global_thread_id == 
            t->CompareAndSwap(my_loc, global_thread_id, global_thread_id+1); 
        if (!success) {
          REMUS_FATAL("Thread {}: CAS failed", global_thread_id); 
        }

        // wait for all threads to finish work 
        t->arrive_control_barrier(total_threads);

        // the above: 
        //   my_loc is a convenience pointer, avoids having to re-compute the same
        //     offset for each Read, Write, and CompareAndSwap
        //   reading and writing have the same general format as in other concurrent
        //     programming environments (ex. transactional memory)
        //   CompareAndSwap is roughly equivalent to std::atmoic<T>::compare_exchange_strong
        //     but it returns the value it observed, not a boolean 



        // veiry the result of the compution 
        // here -- thread 0 of Compute Node 0 can read from the SharedObject to
        //   ensure that each thread did what it was supposed to do 

        // now thread 0 can check everything
        if (global_thread_id == 0) {
          for (uint64_t i = 1; i < total_threads + 1; ++i) {
            auto found = t->Read<uint64_t>(remus::rdma_ptr<uint64_t>(
              root.raw() + offsetof(SharedObject, values[i]))); 
            if (found != i) {
              REMUS_FATAL("in position {}, expected {}, found{}", i, i, found); 
            }
            REMUS_INFO("All checks succeeded");
          }
        }


        // before threads shut down, clean up the data structure

        // reclaim the object before terminating
        if (global_thread_id == 0) {
          t->deallocate(root); 
          t->set_root(remus::rdma_ptr<SharedObject>(nullptr));
        }

        // NOTE -- this code runs BEFORE the ComputeThread is reclaimed
        //   ComputeThread destruction triggers MemoryNode reclamation - wouldn't
        //     want reclamation to be attempted after the MN has already been cleaned up

            }));
    }  

    // the above -- uses the sense-reversing barrier in MN 0 to ensure that no threads
    //   reads the root until ALL threads have reached the barrier
    // the threads on CN 0 won't even run until AFTER CN 0 has written the root
    //   so that's a guaranteed that no thread will read the root before it's 
    //   initialized

    // now, after the loop, join all of the threads
    for (auto &t : local_threads) {
      t.join();
    }

  // for now, Remus uses per-thread free lists when reclaiming memory, and 
  //   favors those on future allocations
  // satisfactory for programs where all threads have the same pattern of
  //   allocations

}
