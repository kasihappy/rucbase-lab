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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点（需要join的表）
    size_t len_;                               // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                // join后获得的记录的字段

    std::vector<Condition> fed_conds_;  // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
    }

    void nextTuple() override {
        for (; !right_->is_end(); right_->nextTuple()) {
            if (left_->is_end())
                left_->beginTuple();
            else
                left_->nextTuple();

            for (; !left_->is_end(); left_->nextTuple()) {
                if (condCheck(get_rec().get())) return;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override { return get_rec(); }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return left_->is_end(); }

    std::unique_ptr<RmRecord> get_rec() {
        std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(len_);
        std::unique_ptr<RmRecord> l_rec = left_->Next();
        std::unique_ptr<RmRecord> r_rec = right_->Next();
        // 组合成一条record
        memset(record->data, 0, record->size);
        memcpy(record->data, l_rec->data, l_rec->size);
        memcpy(record->data + l_rec->size, r_rec->data, r_rec->size);

        return record;
    }

    bool condCheck(const RmRecord *l_record) {
        char *l_val_buf, *r_val_buf;
        const RmRecord *r_record;

        for (auto &condition : fed_conds_) {  // 条件判断
            CompOp op = condition.op;
            int cmp;

            // record和col确定数据位置
            auto l_col = get_col(cols_, condition.lhs_col);  // 左列元数据
            l_val_buf = l_record->data + l_col->offset;      // 确定左数据起点

            if (condition.is_rhs_val) {  // 值
                r_record = condition.rhs_val.raw.get();
                r_val_buf = r_record->data;

                cmp = cond_compare(l_val_buf, r_val_buf, condition.rhs_val.type, l_col->len);
            } else {  // 列
                auto r_col = get_col(cols_, condition.rhs_col);
                r_val_buf = l_record->data + r_col->offset;

                cmp = cond_compare(l_val_buf, r_val_buf, r_col->type, l_col->len);
            }
            if (!op_compare(op, cmp))  // 不满足条件
                return false;
        }
        return true;
    }

    int cond_compare(const char *l_val_buf, const char *r_val_buf, ColType type, int col_len) const {
        int cmp = ix_compare(l_val_buf, r_val_buf, type, col_len);
        return cmp;
    }

    bool op_compare(CompOp op, int cmp) const {
        if (op == OP_EQ) {
            return cmp == 0;
        } else if (op == OP_NE) {
            return cmp != 0;
        } else if (op == OP_LT) {
            return cmp < 0;
        } else if (op == OP_GT) {
            return cmp > 0;
        } else if (op == OP_LE) {
            return cmp <= 0;
        } else if (op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Invalid CompOp");
        }
    }

    size_t tupleLen() const override { return len_; };
    std::string getType() override { return "NestedLoopJoinExecutor"; };
    const std::vector<ColMeta> &cols() const override { return cols_; };
};