#pragma once

#include <memory>
#include <remus/remus.h>

using namespace remus;


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

  /// Construct a LazyListSet, setting its 'This' pointer to a remote memory
  ///   location. Note that every thread in the program could have a unique LazyListSet, 
  ///   but if they all use the same 'This', they'll all access the same remote memory 
  ///
  /// @param This   a LocakFreList produced by a preceeding call to 'New()'
  LazyListSet(const remus::rdma_ptr<LazyListSet> &This) 
      : This((LazyListSet *)((uintptr_t)This)) {}
  

    
  ////// !!! IMPLEMENTING get() !!! 

  // report if 'ptr' has its marked bit set
  bool is_marked(uintptr_t ptr) { return ptr & 1; }

  // clear the low bit of a uintptr_t
  uintptr_t unset_mark(uintptr_t ptr) { return ptr & (UINTPTR_MAX - 1); }

  // set the low bit of a uintptr_t
  uintptr_t set_mark(uintptr_t ptr) { return ptr | 1; }

  // clear the low bit of a pointer
  Node *make_unmarked(Node *ptr) { return (Node *)unset_mark((uintptr_t)ptr); }

  // set the low bit of a pointer
  Node *mark_marked(Node *ptr) { return (Node *)set_mark((uintptr_t)ptr); }

  /// report if a key is present in the set
  ///
  /// @param key  the key to search for
  /// @param ct   the calling thread's Remus context
  /// @return true if it's found, false otherwise
  bool get(const K &key, CT &ct) {
    Node *HEAD = This->head_.load(ct);
    Node *TAIL = This->tail_.load(ct);
    Node *curr = HEAD->next_.load(ct);

    while (curr->key_.load(ct) < key && curr != TAIL)
      curr = make_unmarked(curr->next_.load(ct));
    return ((curr != HEAD) && (curr->key_.load(ct) == key) && !is_marked((uintptr_t)curr->next_.load(ct)));
  }


  //////// !!!!! INSERTING DATA !!!!!!
  
  /// ensure that 'pred' and 'curr' are unmarked, and that 'pred->next_'
  ///     references 'curr'
  ///
  /// NB: assumes 'pred' and 'curr' are made from rdma_ptrs
  ///
  /// @param pred   the first pointer of the pair
  /// @param curr   the second pointer of the pair
  /// @param ct     the calling thread's Remus context
  ///
  /// @return true if the validation succeeds, false otherwise
  bool validate_ptrs(Node *pred, Node *curr, CT &ct) {
    auto pn = pred->next_.load(ct);
    auto cn = curr->next_.load(ct);
    return (!is_marked((uintptr_t)pn) && !is_marked((uintptr_t)cn) && (pn == curr));
  }

  /// insert a key into the set if it doesn't already exist
  ///
  /// @param key    the key to insert
  /// @param ct     the calling thread's Remus context
  ///
  /// @return true if the key was inserted, false if it was already present
  bool insert(const K &key, CT &ct) {
    Node *HEAD = This->head_.load(ct);
    Node *TAIL = This->tail_.load(ct);
    Node *curr, *pred;

    int result, validated, not_val; 
    while (true) {
      pred = HEAD; 
      curr = make_unmarked(pred->next_.load(ct));
      while (curr->key_.load(ct) < key && curr != TAIL) {
        pred = curr; 
        curr = make_unmarked(curr->next_.load(ct));
      }
      pred->acquire(ct);
      curr->acquire(ct);
      validated = validate_ptrs(pred, curr, ct);
      not_val = (curr->key_.load(ct) != key || curr == TAIL);
      result = (validated && not_val); 
      if (result) {
        Node *new_node = ct->New<Node>();
        new_node->init(key, ct);
        new_node->next_.store(curr, ct);
        pred->next_.store(new_node, ct);
      }
      curr->release(ct);
      pred->release(ct);
      if (validated)
        return result; 
    }
  }



  ///// !!!!! REMOVING A KEY !!!!!
  // challenge -- another thread (possibly on another machine) could be 
  //      reading a Node while it's being removed
  // can lead to ABA problems 

  // Remus's current solution: 
  //   can SchedReclaim and object, which places it into a list
  //   then call ReclaimDeferred at some point in the future (such as after
  //      a thread barrier) when you know its safe to reclaim *everything* in the list

  /// remove an entry from the set
  ///
  /// @param key  the key to remove
  /// @param ct   the calling thread's Remus context
  ///
  /// @return true on success, false if key not present
  bool remove(const K &key, CT &ct) {
    Node *TAIL = This->tail_.load(ct);
    Node *pred, *curr;
    int result, validated, isVal;
    while (true) {
      pred = This->head_.load(ct);
      curr = make_unmarked(pred->next_.load(ct));
      while (curr->key_.load(ct) < key && curr != TAIL) {
        pred = curr;
        curr = make_unmarked(curr->next_.load(ct));
      }
      pred->acquire(ct);
      curr->acquire(ct);
      validated = validate_ptrs(pred, curr, ct);
      isVal = key == curr->key_.load(ct) && curr != TAIL;
      result = validated && isVal;
      if (result) {
        curr->next_.store(make_marked(curr->next_.load(ct)), ct);
        pred->next_.store(make_unmarked(curr->next_.load(ct)), ct);
        ct->SchedReclaim(curr);
      }
      curr->release(ct);
      pred->release(ct);
      if (validated)
        return result;
    }
  }



  ////// !!!!!! RECLAIMING A DATA STRUCTURE !!!!!!

  /// "Destruct" the list by reclaiming all of its nodes, and then reclaiming
  ///    its 'This' pointer
  /// It's assumed that this runs in a single-threaded context so that memory
  ///    can be immediately reclaimed
  ///
  /// @param ct   the calling thread's Remus context
  void destroy(CT &ct) {
    Node *curr = This->head_.load(ct); 
    while (curr) {
      Node *next = curr->next_.load(ct);
      ct->Delete(curr);
      curr = next; 
    }
    ct->Delete(This); 
  }
}
