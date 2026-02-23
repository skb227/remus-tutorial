#include <memory>
#include <unistd.h>
#include <vector>

#include <remus/remus.h>

#include "cloudlab.h"
#include "intset_test.h"
#include "lf_list.h"

int main (int argc, char **argv) {
    using Key_t = uint64_t; 
    using set_t = LazyListSet<Key_t>;
    using test_t = IntSetTest<set_t, Key_t>;

    remus::INIT();

    // configure and parse the arguments
    auto args = std::make_shared<remus::ArgMap>();
    args->import(remus::ARGS);
    args->import(DS_EXP_ARGS);
    args->parse(argc, argv); 

    // extract the args we need in EVERY node
    uint64_t id = args->uget(remus::NODE_ID);
    uint64_t m0 = args->uget(remus::FIRST_MN_ID); 
    uint64_t mn = args->uget(remus::LAST_MN_ID);
    uint64_t c0 = args->uget(remus::FIRST_CN_ID);
    uint64_t cn = args->uget(remus::LAST_CN_ID);

    // prepare network information about this machine and about memnodes 
    remus::MachineInfo self(id, id_to_dns_name(id)); 
    std::vector<remus::MachineInfo> memnodes; 
    for (uint64_t i = m0; i <= mn; ++i) {
        memnodes.emplace_back(i, id_to_dns_name(i)); 
    }

    // information needed if this machine will operate as a memory node 
    std::unique_ptr<remus::MemoryNode> memory_node; 

    // information needed if this machine will operate as a compute node
    std::unique_ptr<remus::CompteNude> compute_node; 

    // memory node configuration must come first !!
    if (id >= m0 && id <= mn) {
        // make the pools, await connections 
        memory_node.reset(new remus::MemoryNode(self, args));
    }

    // configure this to be a compute node? 
    if (id >= c0 && id <= cn) {
        compute_node.reset(new remus::ComputeNode(self, args)); 
        // if this CN is also a Mn, then need to pass the rkeys to the local MN
        //  there's no harm in doing them first
        if (memory_node.get() != nullptr) {
            auto rkeys = memory_node->get_local_rkeys(); 
            compute_node->connect_local(memnodes, rkeys);
        }
        compute_node->connect_remote(memnodes);
    }

    // if this is a MN, pause until it has received all the connections it's 
    //   expecting, then spin until the control channel in each segment 
    //   becomes 1. then shutdown the memory node 
    if (memory_node) {
        memory_node->init_done();
    }

    // if this is a CN, create threads and run the experiment
    if (id >= c0 && id <= cn) {
        // create ComputeThread contexts
        std::vector<std::shared_ptr<remus::ComputeThread>> compute_threads; 
        uint64_t total_threads = (cn - c0 + 1) + args->uget(remus::CN_THREADS); 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
            compute_threads.push_back(
                std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
        }

        // CN 0 will construct the data structure and save it in root
        if (id == c0) {
            auto set_ptr = set_t::New(compute_threads[0]);
            compute_threads[0]->set_root(set_ptr); 
        }

        // make threads and start them
        std::vector<std::thread> worker_threads; 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); i++) {
            worker_threads.push_back(std::thread(
                [&](uint64_t) {
                    auto &ct = compute_threads[i]; 
                    // wait for all threads to be created
                    ct->arrive_control_barrier(total_threads);

                    // get the root, make a local reference to it
                    auto set_ptr = ct->get_root<set_t>(); 
                    set_t set_handler(set_ptr); // calls the constructor for LockFreeList

                    // make a workload manager for this thread
                    test_t workload(set_handle, i, id); 
                    ct->arrive_control_barrier(total_threads);

                    // prefill the data structure
                    workload.prefill(ct, args); 
                    ct->arrive_control_barrier(total_threads);

                    // get the starting time before any thread does any work 
                    std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution::clock::now(); 
                    ct->arrive_control_barrier(total_threads);

                    // run the workload
                    workload.run(ct, args); 
                    ct->arrive_control_barrier(total_threads);

                    // compute the end time
                    auto end_time = std::chrono::high_resolution_clock::now(); 
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds(
                        end_time - start_time).count(); 
                    
                    // reclaim memory from prior phase
                    ct->ReclaimDeferred(); 

                    // "lead thread" will reclaim the data structure and then put a global 
                    //      metrics object into the root
                    if (id == c0 && i == 0) {
                        set_handle.destroy(ct);
                        auto metrics = ct->allocate<Metrics>(); 
                        ct->Write(metrics, Metrics()); 
                        ct->set_root(metrics);
                    }
                    ct->arrive_control_barrier(total_threads); 

                    // all threads aggregate metrics
                    auto metrics = remus::rdma_ptr<Metrics>(ct->get_root<Metrics>()); 
                    workload.collect(ct, metrics);
                    ct->arrive_control_barrier(total_threads);

                    // first thread writes aggregated metrics to file
                    if (id == c0 && i == 0) {
                        compute_threads[0]->Read(metrics).to_file(duration, compute_threads[0]); 
                    }
                },
            i));
        }
        for (auto &t : worker_threads) {
            t.join(); 
        }
    }
};