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

#include "ix_defs.h"
#include "transaction/transaction.h"

enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

static const bool binary_search = false;

inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
            return memcmp(a, b, col_len);
        default:
            throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char *a, const char *b, const std::vector<ColType> &col_types,
                      const std::vector<int> &col_lens) {
    int offset = 0;
    for (size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if (res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
    friend class IxIndexHandle;
    friend class IxScan;

   private:
    const IxFileHdr *file_hdr;  // 节点所在文件的头部信息
    Page *page;                 // 存储节点的页面
    IxPageHdr *page_hdr;        // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;  // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;   // page->data的第三部分，指针指向首地址

   public:
    IxNodeHandle() = default;

    IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_) {
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    int get_size() { return page_hdr->num_key; }

    void set_size(int size) { page_hdr->num_key = size; }

    int get_max_size() { return file_hdr->btree_order_ + 1; }

    int get_min_size() { return get_max_size() / 2; }

    int key_at(int i) { return *(int *)get_key(i); }

    /* 得到第i个孩子结点的page_no */
    page_id_t value_at(int i) { return get_rid(i)->page_no; }

    page_id_t get_page_no() { return page->get_page_id().page_no; }

    PageId get_page_id() { return page->get_page_id(); }

    page_id_t get_next_leaf() { return page_hdr->next_leaf; }

    page_id_t get_prev_leaf() { return page_hdr->prev_leaf; }

    page_id_t get_parent_page_no() { return page_hdr->parent; }

    bool is_leaf_page() { return page_hdr->is_leaf; }

    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

    void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

    void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }

    char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

    Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }

    void set_key(int key_idx, const char *key) {
        memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_);
    }

    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }

    int lower_bound(const char *target) const;

    int upper_bound(const char *target) const;

    void insert_pairs(int pos, const char *key, const Rid *rid, int n);

    page_id_t internal_lookup(const char *key);

    bool leaf_lookup(const char *key, Rid **value);

    int insert(const char *key, const Rid &value);

    // 用于在结点中的指定位置插入单个键值对
    void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }

    void erase_pair(int pos);

    int remove(const char *key);

    /**
     * @brief used in internal node to remove the last key in root node, and return the last child
     *
     * @return the last child
     */
    page_id_t remove_and_return_only_child() {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(IxNodeHandle *child) {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            if (get_rid(rid_idx)->page_no == child->get_page_no()) {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }

    /*-------------------------------------辅助函数------------------------------------------------------*/
#define KEY_SIZE (file_hdr->col_tot_len_)
#define RID_SIZE (sizeof(Rid))
    enum operators { LOWER, UPPER };  // lower means ">" while upper means ">="

    /**
     * @brief 通过位置查找key和rid并返回给上层
     *
     * @param pos 目标位置
     * @return [key, rid] 查找对应值
     */
#define getPositionKeyAndRid()    \
    char *pos_key = get_key(pos); \
    Rid *pos_rid = get_rid(pos);

    /**
     * @brief 二分查找与target相关的key
     *
     * @param target 目标target
     * @param op LOWER意味着查找第一个>=target的key
     * UPPER 意味着查找第一个>target的key
     * @return 查找到的key index
     */
    int halfFind(const char *target, operators op) const {
        int left = 0, mid, right = page_hdr->num_key;
        while (left < right) {
            mid = (left + right) / 2;
            int compareRes = ix_compare(target, get_key(mid), file_hdr->col_types_, file_hdr->col_lens_);
            if (compareRes >= 0) {
                if (op == LOWER && compareRes == 0) {
                    right = mid;
                } else {
                    left = mid + 1;
                }
            } else {
                right = mid;
            }
        }
        return left;
    }

    /**
     * @brief 在叶子节点中获取目标key所在位置
     * 并判断目标key是否存在
     * @param key 目标key
     * @return 当前key对应的index，若不存在，返回-1
     */
    int locateKeyIndex(const char *key) {
        // 1. 在叶子节点中获取目标key所在位置
        int index = lower_bound(key);

        // 2. 判断目标key是否存在
        if (index != get_size()) {  // 观察get_key函数，发现没安全有保护，直接返回keys + key_idx *
                                    // file_hdr->col_tot_len_，故添加这一层保险
            if (!ix_compare(key, get_key(index), file_hdr->col_types_, file_hdr->col_lens_)) {
                return index;
            }
        }
        return -1;
    }

    /**
     * @brief 设置一个节点为叶子节点
     *
     * @param isLeaf bool,是否是叶子
     * */
    void set_leaf(bool isLeaf) { page_hdr->is_leaf = isLeaf; }

    // 判断节点是否满
    bool is_full() { return this->get_size() == this->get_max_size(); }

    // 返回keys
    char *get_keys() { return keys; }
    /*----------------------------------------结束------------------------------------------------------*/
};

/* B+树 */
class IxIndexHandle {
    friend class IxScan;
    friend class IxManager;

   private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    int fd_;               // 存储B+树的文件
    IxFileHdr *file_hdr_;  // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    std::mutex root_latch_;

   public:
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

    // for search
    bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);

    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                   bool find_first = false);

    // for insert
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction);

    IxNodeHandle *split(IxNodeHandle *node);

    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

    // for delete
    bool delete_entry(const char *key, Transaction *transaction);

    bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                  bool *root_is_latched = nullptr);
    bool adjust_root(IxNodeHandle *old_root_node);

    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                  Transaction *transaction, bool *root_is_latched);

    Iid lower_bound(const char *key);

    Iid upper_bound(const char *key);

    Iid leaf_end() const;

    Iid leaf_begin() const;

   private:
    // 辅助函数
    void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }

    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    // for get/create node
    IxNodeHandle *fetch_node(int page_no) const;

    IxNodeHandle *create_node();

    // for maintain data structure
    void maintain_parent(IxNodeHandle *node);

    void erase_leaf(IxNodeHandle *leaf);

    void release_node_handle(IxNodeHandle &node);

    void maintain_child(IxNodeHandle *node, int child_idx);

    // for index test
    Rid get_rid(const Iid &iid) const;

    /**
     * @brief 获取最右叶子节点的页面编号
     *
     * @return 最右叶子节点的page number
     * */
    page_id_t get_last_leaf() { return file_hdr_->last_leaf_; }

    /**
     * @brief 设置最右节点的页面编号
     *
     * @param pageNo 要设置的编号
     * */
    void set_last_leaf(page_id_t pageNo) { file_hdr_->last_leaf_ = pageNo; }

    /**
     * @brief 如果若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf
     * 用于节点分裂的时候
     *
     * @param {originNode} 原节点
     * @param {newNode} 分裂产生的节点
     * */
    void updateLastLeaf(IxNodeHandle *originNode, IxNodeHandle *newNode) {
        if (originNode->get_page_no() == get_last_leaf()) {
            set_last_leaf(newNode->get_page_no());
        }
    }

    /**
     * @brief 插入节点的时候，更新相关节点的前驱和后继
     *
     * @param {originLeaf} 插入在该节点之后
     * @param {newLeaf} 要插入的节点
     * @param {keep} bool, 是否保留原节点
     * */
    void insertLeaf(IxNodeHandle *originNode, IxNodeHandle *newNode, bool keep) {
        newNode->set_next_leaf(originNode->get_next_leaf());
        if (keep) {
            newNode->set_prev_leaf(originNode->get_page_no());
            originNode->set_next_leaf(newNode->get_page_no());
        } else  // node与其前驱合并, 什么都不用做
            ;

        if (newNode->get_next_leaf() != INVALID_PAGE_ID) {
            // 如果下一个叶子节点存在
            auto nextNode = fetch_node(newNode->get_next_leaf());
            nextNode->set_prev_leaf(newNode->get_page_no());
            // 不unpin也能通过?
            buffer_pool_manager_->unpin_page(nextNode->get_page_id(), true);
        }
    }

    page_id_t getRootPageNo() { return file_hdr_->root_page_; }

    /**
     * @brief 将原节点的键值对平均分配
     *
     * @param {newNode} 分裂出的节点
     * @param {originNode} 原节点
     * */
    void evenlyDistributePairs(IxNodeHandle *newNode, IxNodeHandle *originNode) {
        int newKeyNum = originNode->get_size() / 2;
        // 初始化新节点的page_hdr内容
        newNode->set_leaf(originNode->is_leaf_page());
        newNode->set_size(originNode->get_size() - newKeyNum);
        originNode->set_size(newKeyNum);
        newNode->set_parent_page_no(originNode->get_parent_page_no());
        // 为新节点分配键值对，更新旧节点的键值对数记录
        for (int i = 0; i < newNode->get_size(); i++) {
            newNode->set_key(i, originNode->get_key(newKeyNum + i));
            newNode->set_rid(i, *originNode->get_rid(newKeyNum + i));
        }
    }

    /**
     * @brief 初始化新的根节点
     * 将其设置为分裂后两节点的父亲节点
     *
     * @param {root} 创建的新的根节点
     * @param {left} 将要设置为它的左孩子的节点
     * @param {right} 将要设置为它的右孩子的节点
     *
     * @note 一般来说，根节点分裂，left为原节点，right为新节点
     * */
    void setNewRoot(IxNodeHandle *root, IxNodeHandle *left, IxNodeHandle *right) {
        root->set_leaf(false);
        root->set_size(2);
        root->set_parent_page_no(INVALID_PAGE_ID);
        update_root_page_no(root->get_page_no());
        root->set_key(0, left->get_key(0));
        root->set_rid(0, Rid{left->get_page_no(), -1});  // 此处使用索引获取rid是不行的，这是为什么?
        root->set_key(1, right->get_key(0));
        root->set_rid(1, Rid{right->get_page_no(), -1});
        // 将newRoot的孩子节点的父节点更新为newRoot
        for (int i = 0; i < root->get_size(); i++) {
            maintain_child(root, i);
        }

        buffer_pool_manager_->unpin_page(root->get_page_id(), true);
    }

    /**
     * @brief 若一个节点满了，分裂它
     *
     * @param {node} 需要操作的节点
     * @param {transaction} 事务指针
     *
     * @note 如果是叶子节点，还要考虑更新last_leaf
     * */
    void splitNodeIfFull(IxNodeHandle *node, Transaction *transaction) {
        if (node->is_full()) {
            // 分裂
            IxNodeHandle *newNode = split(node);
            // 更新父节点
            insert_into_parent(node, newNode->get_keys(), newNode, transaction);
            // 若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf
            if (node->is_leaf_page()) {
                updateLastLeaf(node, newNode);
                buffer_pool_manager_->unpin_page(node->get_page_id(), true);
                buffer_pool_manager_->unpin_page(newNode->get_page_id(), true);
            }
        }
    }

    /**
     * @brief 寻找node结点的兄弟结点（优先选取前驱结点）
     *
     * @param {index} node节点的位置
     * @param {parent} node节点的父亲
     * @return 返回node结点的兄弟结点（优先选取前驱结点）
     */
    IxNodeHandle *getNeighbor(int index, IxNodeHandle *parent) {
        if (index) {
            // 优先选取前驱结点
            return fetch_node(parent->get_rid(index - 1)->page_no);
        }
        return fetch_node(parent->get_rid(index + 1)->page_no);
    }

    // 交换两个节点
    void switchNode(IxNodeHandle **node, IxNodeHandle **neighbor) {
        IxNodeHandle *temp = *neighbor;
        *neighbor = *node;
        *node = temp;
    }
};