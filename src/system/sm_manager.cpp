/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta* new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    // 进入名为db_name的目录
    chdir(db_name.c_str());
    // 加载DB元数据
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;

    // 加载数据库相关文件
    for (auto& entry : db_.tabs_) {
        std::string tab_name = entry.first;
        // 加载每张表的数据文件
        fhs_[tab_name] = rm_manager_->open_file(tab_name);
        // 加载索引
        for (auto& index : db_.tabs_[tab_name].indexes) {
            ihs_.emplace(ix_manager_->get_index_name(tab_name, index.cols),
                         ix_manager_->open_index(tab_name, index.cols));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // 将数据库输入刷入磁盘中
    flush_meta();
    // 关闭表数据文件
    for (auto& entry : fhs_) {
        const RmFileHandle* file_handle = entry.second.get();
        rm_manager_->close_file(file_handle);
    }
    // 关闭索引文件
    for (auto& entry : ihs_) {
        const IxIndexHandle* index_handle = entry.second.get();
        ix_manager_->close_index(index_handle);
    }

    // 删除已打开信息
    db_.name_.clear();
    db_.tabs_.clear();
    fhs_.clear();
    ihs_.clear();

    // 回到上级目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto& col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (db_.is_table(tab_name)) {
        // 文件数据
        const RmFileHandle* file_handle = fhs_[tab_name].get();
        rm_manager_->close_file(file_handle);
        rm_manager_->destroy_file(tab_name);  // 数据文件
        // 索引数据
        for (auto& index : db_.tabs_[tab_name].indexes) {
            if (ix_manager_->exists(tab_name, index.cols)) {
                // 关闭索引
                std::string idx_name = ix_manager_->get_index_name(tab_name, index.cols);
                const IxIndexHandle* ih = ihs_[idx_name].get();
                ix_manager_->close_index(ih);
                // 删除索引文件
                ix_manager_->destroy_index(tab_name, index.cols);
                // 删除索引记录
                ihs_.erase(idx_name);
            }
        }
        // 存在信息
        db_.tabs_.erase(tab_name);
        fhs_.erase(tab_name);
    } else {
        throw TableNotFoundError(tab_name);
    }
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 索引是否存在
    if (ix_manager_->exists(tab_name, col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }
    // 创建索引
    std::vector<ColMeta> idx_cols;
    for (auto& col_name : col_names) {
        idx_cols.push_back(*(db_.get_table(tab_name).get_col(col_name)));
    }
    ix_manager_->create_index(tab_name, idx_cols);
    // 打开，放入ihs
    std::string ix_name = ix_manager_->get_index_name(tab_name, col_names);
    ihs_.emplace(ix_name, ix_manager_->open_index(tab_name, col_names));
    // 更新indexes
    IndexMeta idx_meta;
    idx_meta.tab_name = tab_name;
    idx_meta.col_tot_len = 0;
    for (auto col_meta : idx_cols) {
        idx_meta.col_tot_len += col_meta.len;
    }
    idx_meta.col_num = idx_cols.size();
    idx_meta.cols = idx_cols;

    db_.tabs_[tab_name].indexes.push_back(idx_meta);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 关闭索引文件
    std::string idx_name = ix_manager_->get_index_name(tab_name, col_names);
    const IxIndexHandle* ih = ihs_[idx_name].get();
    ix_manager_->close_index(ih);
    // 删除索引文件
    ix_manager_->destroy_index(tab_name, col_names);
    // 从ihs中删除
    ihs_.erase(ix_manager_->get_index_name(tab_name, col_names));
    // 更新indexes
    auto idx_meta = db_.get_table(tab_name).get_index_meta(col_names);
    db_.get_table(tab_name).indexes.erase(idx_meta);
}