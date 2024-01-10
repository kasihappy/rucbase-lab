/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    return addLockOnTable(txn, tab_fd, 1);
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    return addLockOnTable(txn, tab_fd, 2);
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    return addLockOnTable(txn, tab_fd, 3);
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    checkAndSetState(txn, true);

    LockRequestQueue* lock_request_queue = &lock_table_[lock_data_id];
//    auto it = lock_request_queue->request_queue_.begin();
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            switch (it->lock_mode_) {
                case LockMode::SHARED:
                    lock_request_queue->shared_lock_num_--;
                    break;
                case LockMode::INTENTION_EXCLUSIVE:
                    lock_request_queue->IX_lock_num_--;
                    break;
                case LockMode::S_IX:
                    lock_request_queue->shared_lock_num_--;
                    lock_request_queue->IX_lock_num_--;
                    break;
                default:
                    break;
            }
            lock_request_queue->request_queue_.erase(it);
            break;
        }
    }

    // 确定queue中级别最大的锁，赋值group_lock_mode_
    // 分对对应X, SIX, S, IX, IS
    bool level[6] = {0, 0, 0, 0, 0, 0};
    for (auto iter : lock_request_queue->request_queue_) {
        switch (iter.lock_mode_) {
            case LockMode::SHARED:
                level[2] = true;
                break;
            case LockMode::EXLUCSIVE:
                level[0] = true;
                break;
            case LockMode::INTENTION_SHARED:
                level[5] = true;
                break;
            case LockMode::INTENTION_EXCLUSIVE:
                level[4] = true;
                break;
            case LockMode::S_IX:
                level[1] = true;
                break;
        }
    }

    if (level[0]) {
        lock_request_queue->group_lock_mode_ = GroupLockMode::X;
    } else if (level[1]) {
        lock_request_queue->group_lock_mode_ = GroupLockMode::SIX;
    } else if (level[2]) {
        lock_request_queue->group_lock_mode_ = GroupLockMode::S;
    } else if (level[3]) {
        lock_request_queue->group_lock_mode_ = GroupLockMode::IX;
    } else if (level[4]) {
        lock_request_queue->group_lock_mode_ = GroupLockMode::IS;
    } else {
        lock_request_queue->group_lock_mode_ = GroupLockMode::NON_LOCK;
    }
    return true;
}