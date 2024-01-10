/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <condition_variable>
#include <mutex>

#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXLUCSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX };

    /* 事务的加锁申请 */
    class LockRequest {
       public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;     // 申请加锁的事务ID
        LockMode lock_mode_;  // 事务申请加锁的类型
        bool granted_;        // 该事务是否已经被赋予锁
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
       public:
        std::list<LockRequest> request_queue_;  // 加锁队列
        std::condition_variable cv_;  // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;  // 加锁队列的锁模式

        int shared_lock_num_ = 0;  // S数量
        int IX_lock_num_ = 0;      // IX数量
    };

   public:
    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    bool unlock(Transaction* txn, LockDataId lock_data_id);

    void checkAndSetState(Transaction* txn, bool stage) {
        auto state = txn->get_state();
        if (!stage) {
            if (state == TransactionState::DEFAULT) {
                // 开启两阶段封锁协议的封锁阶段
                txn->set_state(TransactionState::GROWING);
            } else if (state== TransactionState::GROWING) {
                // pass
            } else {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
            }
        } else {
            switch (state) {
                case TransactionState::GROWING:
                    txn->set_state(TransactionState::SHRINKING);
                case TransactionState::DEFAULT:
                case TransactionState::SHRINKING:
                    break;
                default:
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
            }
        }
    }

    void addLockIfNoPrev(LockDataId lockId) {
        if (lock_table_.count(lockId) == 0) {
            lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lockId), std::forward_as_tuple());
        }
    }

    static void noWait(LockRequestQueue* lock_request_queue, txn_id_t id, int type) { // 这里可以搞一个enum
        if (type == 1) { // S
            if (lock_request_queue->group_lock_mode_ == GroupLockMode::IX ||
                lock_request_queue->group_lock_mode_ == GroupLockMode::X ||
                lock_request_queue->group_lock_mode_ == GroupLockMode::SIX) {
                throw TransactionAbortException(id, AbortReason::DEADLOCK_PREVENTION);
            }
        } else if (type == 2) { // X
            if (lock_request_queue->group_lock_mode_ != GroupLockMode::NON_LOCK) {
                throw TransactionAbortException(id, AbortReason::DEADLOCK_PREVENTION);
            }
        } else if (type == 3) { // IX
            if (lock_request_queue->group_lock_mode_ == GroupLockMode::S ||
                lock_request_queue->group_lock_mode_ == GroupLockMode::X ||
                lock_request_queue->group_lock_mode_ == GroupLockMode::SIX) {
                throw TransactionAbortException(id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    static void addLock(LockRequestQueue* lock_request_queue, Transaction* txn, LockDataId lockId, int type) {
        auto txnId = txn->get_transaction_id();
        LockRequest lock_request = LockRequest{txnId, LockMode::SHARED};
        if (type == 1) {
            lock_request = LockRequest{txnId, LockMode::SHARED};
            lock_request_queue->group_lock_mode_ = GroupLockMode::S;
            lock_request_queue->shared_lock_num_++;
        } else if (type == 2) {
            lock_request = LockRequest{txnId, LockMode::EXLUCSIVE};
            lock_request_queue->group_lock_mode_ = GroupLockMode::X;
        } else if (type == 3) {
            lock_request = LockRequest{txnId, LockMode::INTENTION_EXCLUSIVE};
            lock_request_queue->group_lock_mode_ = GroupLockMode::IX;
            lock_request_queue->IX_lock_num_++;
        }
        lock_request.granted_ = true;
        lock_request_queue->request_queue_.emplace_back(lock_request);
        txn->get_lock_set()->emplace(lockId);
    }

    bool lockGreaterThan(LockMode lock, int type) {
        if (type == 1) {
            if (lock == LockMode::SHARED || lock == LockMode::EXLUCSIVE || lock == LockMode::S_IX)
                return true;
        } else if (type == 2) {
            if (lock == LockMode::EXLUCSIVE)
                return true;
        } else if (type == 3) {
            if (lock == LockMode::EXLUCSIVE || lock == LockMode::S_IX || lock == LockMode::INTENTION_EXCLUSIVE)
                return true;
        }
        return false;
    }

    bool upgradeLockMode(LockRequestQueue* lock_request_queue, LockRequest& lock_request, int type) {
        LockMode lock = lock_request.lock_mode_;
        if (type == 1) { // 当前事务试图给当前项加S锁， 两个特殊情况
            if (lock == LockMode::INTENTION_SHARED) {
                // 已经有IS锁了，升级为S锁
                lock_request.lock_mode_ = LockMode::SHARED;
                lock_request_queue->group_lock_mode_ = GroupLockMode::S;
                lock_request_queue->shared_lock_num_++;
                return true;
            } else if (lock == LockMode::INTENTION_EXCLUSIVE) {
                // 已经有IX锁了，且其他事务不持有IX锁，升级为SIX锁
                lock_request.lock_mode_ = LockMode::S_IX;
                lock_request_queue->group_lock_mode_ = GroupLockMode::SIX;
                lock_request_queue->shared_lock_num_++;
                return true;
            } else if (lockGreaterThan(lock, 1)) {
                return true;
            }
        } else if (type == 2) {
            if (lock_request_queue->request_queue_.size() == 1) {
                // 只有当前事务与此数据相关，才能升级
                lock_request.lock_mode_ = LockMode::EXLUCSIVE;
                lock_request_queue->group_lock_mode_ = GroupLockMode::X;
                return true;
            } else if (lockGreaterThan(lock, 2)) {
                return true;
            }
        } else if (type == 3) {
            if (lock == LockMode::INTENTION_SHARED) {
                lock_request.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                lock_request_queue->group_lock_mode_ = GroupLockMode::IX;
                lock_request_queue->IX_lock_num_++;
                return true;
            } else if (lock == LockMode::SHARED && lock_request_queue->shared_lock_num_ == 1) {
                // 只有当前事务对该项加了读锁
                lock_request.lock_mode_ = LockMode::S_IX;
                lock_request_queue->group_lock_mode_ = GroupLockMode::SIX;
                lock_request_queue->IX_lock_num_++;
                return true;
            } else if (lockGreaterThan(lock, 2)) {
                return true;
            }
        }
        return false;
    }

    bool addLockOnTable(Transaction* txn, int tab_fd, int type) {
        checkAndSetState(txn, false);

        LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
        // 如果之前没有锁，加一个锁
        addLockIfNoPrev(lock_data_id);
        LockRequestQueue* lock_request_queue = &lock_table_[lock_data_id];
        auto txnId = txn->get_transaction_id();

        for (auto& iter : lock_request_queue->request_queue_)
            if (iter.txn_id_ == txnId)
                if (upgradeLockMode(lock_request_queue, iter, type))
                    return true;

        // no wait
        noWait(lock_request_queue, txnId, type);

        // 加锁
        addLock(lock_request_queue, txn, lock_data_id, type);
        return true;
    }

   private:
    std::mutex latch_;                                             // 用于锁表的并发
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;  // 全局锁表
};
