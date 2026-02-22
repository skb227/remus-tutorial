#pragma once

#include <memory>
#include <remus/remus.h>

using namespace remus;

/// a lock-based, sorted, list-based set with wait-free contains
///
/// @tparam K 	the type for keys stored in the map, must be default constructable
template <typename K> class LazyListSet {

  /// allocate a LazyList in remote memory and initialize it 
  ///
  /// @param ct   the calling thread's Remus context
  ///
  /// @return  	  an rdma_ptr to the newly allocate, initialized, empty list
  static rdma_ptr<LazyListSet> New(CT &ct) {
    auto tail = ct->New<Node>(); 
    tail->init(K(), ct); 
    auto head = ct->New<Node>();
    head->init(K(), ct);
    head->next_.store(tail, ct);
    auto list = ct->New<LazyListSet>(); 
    list->head_.store(tail, ct); 
    return rdma_ptr<LazyListSet>((uintptr_t)list); 
  }

  /// Construct a LazyListSet, settings its 'This' 
}

/// define the type for nodes: 
 
using CT = std::shared_ptr<ComputeThread>;

// a node in the linked list
struct Node {
  Atomic<K> key_; 		// the key stored in this node
  Atomic<Node *> next_;	// pointer to the next node
  Atomic<uint64_t> lock_;	// a test-and-set lock

  // initialize the current node, assume it's called with 'this' being
  //   a remote pointer's value
  //
  // @param k   the key to store in this node
  // @param ct  the calling thread's Remus context
  void init(const K &k, CT &ct) {
    key_.store(k, ct);
    lock_.store(false, ct);
    next_.store(nullptr, ct);
  }

  // lock this node, assume it's not already locked by the calling thread
  // also assume it's called with 'this' being a remote pointer's value
  //
  // @param ct   the calling thread's Remus context
  void acquire(CT &ct) {
    while (true) {
      if (lock_.compare_exchange_weak(0, 1, ct)) {
        break; 
      }
      while (lock_.load(ct) == 1) {
      }
    }
  }

  // unlock this node, assumes it's called by the thread who locked it, and 
  //   that the node is locked
  // also assumes that it's called with 'this' being a remote pointer's value
  //
  // @param ct   the calling thread's Remus context
  void release(CT &ct) { lock_.store(0, ct);  }
};


// the head and tail sentinel pointers will also be Atomic
Atomic<Node *> head_;   // the list head sentinel   (use through This)
Atomic<Node *> tail_;   // the list tail sentinel   (use through This)
			
// establish a special version of the 'this' pointer
LazyListSet *This;    // the "this" pointer to use for data acesses
