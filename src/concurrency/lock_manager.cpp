//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"

#include "concurrency/transaction.h"

#include "concurrency/transaction_manager.h"

namespace bustub {

  auto LockManager::LockTable(Transaction * txn, LockMode lock_mode,
    const table_oid_t & oid) -> bool {
    assert(txn -> GetState() != TransactionState::COMMITTED && txn -> GetState() != TransactionState::ABORTED);

    if (txn -> GetState() == TransactionState::SHRINKING && (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn -> SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn -> GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn -> SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
    if (txn -> GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (txn -> GetState() == TransactionState::SHRINKING) {
        txn -> SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (txn -> GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (txn -> GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
        txn -> SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    table_lock_map_latch_.lock();  //Lock The LockManager so u can read from it
   
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      //This is the first lock request to this resource 
      auto lockQueue = std::make_shared < LockRequestQueue > ();
      table_lock_map_[oid] = lockQueue;
    }  
      auto lockQueue = table_lock_map_.find(oid) -> second;
      lockQueue -> latch_.lock();  //Lock The Table Linked List
      // table_lock_map_.find(oid) -> second -> request_queue_.push_back(lock_request);
    
    table_lock_map_latch_.unlock();

    //I locked the linked list on the table to search if i already requested a lock (Upgrade Potential) 
    //I unlocked the lock manager i don't wanna block another threads accessing different table_ids
    int currentIndex = -1;
    for (auto lockReq : lockQueue->request_queue_) {
      currentIndex++;
      //lockQueue for the whole queue with properties   lockReq is the request inside the queue
      if (lockReq->txn_id_ == txn->GetTransactionId()) {
        //I already requested this lock before
        if (lock_mode == lockReq->lock_mode_) {
          //if the same lock mode 
          lockQueue -> latch_.unlock();
          return true;
        }
        if (lockQueue->upgrading_ != INVALID_TXN_ID) {
          //Only 1 txn can upgrade  LEHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH
          lockQueue->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }
          if (!(lockReq->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(lockReq->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(lockReq->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(lockReq->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        lockQueue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      //Now i will upgrade
        lockQueue->request_queue_.remove(lockReq);
        RemoveTransactionTableLocks(txn, lockReq);
        auto upgrageRequest = new LockRequest(txn -> GetTransactionId(), lock_mode, oid);
        std::list<LockRequest *>::iterator it = lockQueue->request_queue_.begin();
        advance(it, currentIndex);
        lockQueue->request_queue_.insert(it, upgrageRequest);
        lockQueue->upgrading_ = txn->GetTransactionId();

        //Wait for granting
        std::unique_lock<std::mutex> uqLock(lockQueue->latch_, std::adopt_lock);
        while (!CheckMyTurn(lockQueue->request_queue_, upgrageRequest)) {
        lockQueue->cv_.wait(uqLock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lockQueue->upgrading_ = INVALID_TXN_ID;
          lockQueue->request_queue_.remove(upgrageRequest);
          lockQueue->cv_.notify_all();
          return false;

        }
        }
        //I Got the Turn here
          lockQueue->upgrading_ = INVALID_TXN_ID;
          upgrageRequest->granted_ = true;
           UpdateTableLockSet(txn, upgrageRequest);
       if (lock_mode != LockMode::EXCLUSIVE) {
        lockQueue->cv_.notify_all();
      }
      return true;
      }
    }

    LockRequest * lockReq = new LockRequest(txn -> GetTransactionId(), lock_mode, oid);
    lockQueue -> request_queue_.push_back(lockReq);
      std::unique_lock<std::mutex> uqLock(lockQueue->latch_, std::adopt_lock);
       while (!CheckMyTurn(lockQueue->request_queue_, lockReq)) {
        lockQueue->cv_.wait(uqLock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lockQueue->request_queue_.remove(lockReq);
          lockQueue->cv_.notify_all();
          return false;
        }
        }
      
        lockReq->granted_ = true;
        UpdateTableLockSet(txn, lockReq);
        if (lock_mode != LockMode::EXCLUSIVE) {
              lockQueue->cv_.notify_all();
        }
        return true;
  }

  auto LockManager::UnlockTable(Transaction * txn,
    const table_oid_t & oid) -> bool {
  // fetch the request queue on this resource
  table_lock_map_latch_.lock();

  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // make sure rows are unlocked before unlocking the table
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();

  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_request_queue = table_lock_map_[oid];

  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // find the request to be unlocked
 
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
 
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      // update txn state based on isolation level
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      // update txn lock set
      RemoveTransactionTableLocks(txn, lock_request);
      return true;
    }
  }

  // not holding the resource
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  }

  auto LockManager::LockRow(Transaction * txn, LockMode lock_mode,
    const table_oid_t & oid,
      const RID & rid) -> bool {
        //Initial Conditions
    assert(txn -> GetState() != TransactionState::COMMITTED && txn -> GetState() != TransactionState::ABORTED);
    if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
    if (txn -> GetState() == TransactionState::SHRINKING && lock_mode == LockMode::EXCLUSIVE) {
      txn -> SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn -> GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED) {
        txn -> SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
    if (txn -> GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (txn -> GetState() == TransactionState::SHRINKING) {
        txn -> SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (txn -> GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (txn -> GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED) {
        txn -> SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn -> GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    
     if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  //lazm de s7? LEEEEEEEH
    if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedLocked(oid) && !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
       
    //End Initial Conditions
    row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.find(rid)->second;

  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // identify whether there is a lock on this resource
  int currentIndex = -1;
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
  currentIndex++;
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // identify whether the lock upgrade is compatible
      if (!(lock_request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) &&
          !(lock_request->lock_mode_ == LockMode::SHARED && lock_mode == LockMode::EXCLUSIVE) &&
          !(lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE) &&
          !(lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // insert upgrade lock request
      lock_request_queue->request_queue_.remove(lock_request);
      RemoveTransactionRowLocks(txn, lock_request);
      auto upgrade_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
       std::list<LockRequest *>::iterator it = lock_request_queue->request_queue_.begin();
 
        advance(it, currentIndex);
        lock_request_queue->request_queue_.insert(it, upgrade_lock_request);
    
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      // wait to be granted
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!CheckMyTurn(lock_request_queue->request_queue_, upgrade_lock_request)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      // update lock request and txn lock set
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      UpdateRowLockSet(txn, upgrade_lock_request);

      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  // push back new lock request
  auto lock_request =  new LockRequest (txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);

  // wait to be granted
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!CheckMyTurn(lock_request_queue->request_queue_, lock_request)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  // update lock request and txn lock set
  lock_request->granted_ = true;
  UpdateRowLockSet(txn, lock_request);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

   


    return true;
  }

  auto LockManager::UnlockRow(Transaction * txn,
    const table_oid_t & oid,
      const RID & rid) -> bool {
   // fetch the request queue on this resource
  row_lock_map_latch_.lock();

  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];

  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // find the request to be unlocked
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      // update txn state based on isolation level
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      // update txn lock set
      RemoveTransactionRowLocks(txn, lock_request);
      return true;
    }
  }

  // not holding the resource
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  }

  void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

  void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

  auto LockManager::HasCycle(txn_id_t * txn_id) -> bool {
    return false;
  }

  auto LockManager::GetEdgeList() -> std::vector < std::pair < txn_id_t, txn_id_t >> {
    std::vector < std::pair < txn_id_t,
    txn_id_t >> edges(0);
    return edges;
  }

  void LockManager::RunCycleDetection() {
    while (enable_cycle_detection_) {
      std::this_thread::sleep_for(cycle_detection_interval);
      { // TODO(students): detect deadlock
      }
    }
  }

  void LockManager::RemoveTransactionTableLocks(Transaction *txn, LockRequest* lock_request) {
    switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
  }
  
  }
      void  LockManager::RemoveTransactionRowLocks(Transaction *txn, LockRequest* lock_request) {
            switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (txn->GetSharedRowLockSet()->find(lock_request->oid_) == txn->GetSharedRowLockSet()->end()) {
         break;
      }
        txn->GetSharedRowLockSet()->find(lock_request->oid_)->second.erase(lock_request->rid_);
      break;
    case LockMode::EXCLUSIVE:
      if (txn->GetExclusiveRowLockSet()->find(lock_request->oid_) == txn->GetExclusiveRowLockSet()->end()) {
          break;
      }
   
        txn->GetExclusiveRowLockSet()->find(lock_request->oid_)->second.erase(lock_request->rid_);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }


      }
  bool LockManager::CheckMyTurn(std::list<LockRequest *>& lockReqList, LockRequest * lockReq)  {
    //Here i got notified that some one leaved the lock is it my turn? 
    for (auto currentLockReq : lockReqList) {
      if (currentLockReq->txn_id_ == lockReq->txn_id_) {
        //Same txn so its my turn
        return true;
      } else if (currentLockReq->granted_) {
          if (lockReq->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          if (lockReq->lock_mode_ == LockMode::SHARED && currentLockReq->lock_mode_ != LockMode::SHARED &&  currentLockReq->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
      if (lockReq->lock_mode_ == LockMode::INTENTION_SHARED &&  currentLockReq->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          if (lockReq->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && currentLockReq->lock_mode_ == LockMode::SHARED || currentLockReq->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              currentLockReq->lock_mode_ == LockMode::EXCLUSIVE) {
                return false;
              }
          if (lockReq->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && currentLockReq->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }

      } else {
        return false;
      }
    }  
    return false;
  }
  void LockManager::UpdateTableLockSet(Transaction *txn, LockRequest *lock_request ) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
    
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
    
      break;
    case LockMode::EXCLUSIVE:
     
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
    
      break;
    case LockMode::INTENTION_SHARED:
      
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      
      break;
    case LockMode::INTENTION_EXCLUSIVE:
     
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
     
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
    
      break;
  }


  }

  void LockManager::UpdateRowLockSet(Transaction *txn,  LockRequest *lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (txn->GetSharedRowLockSet()->find(lock_request->oid_) == txn->GetSharedRowLockSet()->end()) {
        txn->GetSharedRowLockSet()->insert(std::make_pair(lock_request->oid_, std::unordered_set<RID>()));
      }
 
        txn->GetSharedRowLockSet()->find(lock_request->oid_)->second.insert(lock_request->rid_);
     
      break;
    case LockMode::EXCLUSIVE:
      if (txn->GetExclusiveRowLockSet()->find(lock_request->oid_) == txn->GetExclusiveRowLockSet()->end()) {
        txn->GetExclusiveRowLockSet()->insert(std::make_pair(lock_request->oid_, std::unordered_set<RID>()));
      }

        txn->GetExclusiveRowLockSet()->find(lock_request->oid_)->second.insert(lock_request->rid_);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

  } // namespace bustub