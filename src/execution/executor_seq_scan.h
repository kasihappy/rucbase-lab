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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;  // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        context_ = context;
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);  // 初始化迭代器
        rid_ = scan_->rid();

        while (!scan_->is_end()) {
            if (condCheck(fh_->get_record(rid_, context_).get())) break;
            scan_->next();
            rid_ = scan_->rid();
        }
    }

    void nextTuple() override {
        // 条件判断
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            if (condCheck(fh_->get_record(rid_, context_).get())) break;
        }
    }

    bool condCheck(const RmRecord *l_record) {
        char *l_val_buf, *r_val_buf;
        const RmRecord *r_record;

        for (auto &condition : conds_) {  // 条件判断
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

    bool is_end() const override { return scan_->is_end(); }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }
    // 补齐，否则段错误
    size_t tupleLen() const override { return len_; };
    std::string getType() override { return "SeqScanExecutor"; };
    const std::vector<ColMeta> &cols() const override { return cols_; };
};