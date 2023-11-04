/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

#include <iostream>

#define getParams(page) 						\
	int fd = page->get_page_id().fd;			\
    int page_no = page->get_page_id().page_no;	\
    char* offset = page->get_data();			\
    int num_bytes = PAGE_SIZE;

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // Todo:
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面
	if (free_list_.empty()) { // free page exists, get frame
    	int frameId;
    	if (replacer_->victim(&frameId)) {
    		// delete the needed free page
    		page_table_.erase(pages_[frameId].get_page_id());
    		*frame_id = frameId;
    		return true;
    	}
    } else { // no free page, lru
    	*frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    return false;
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page
 * table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id) {
    // Todo:
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id
    if (page->is_dirty()) {
    	getParams(page)
    	// write dirty page to disk
        disk_manager_->write_page(fd, page_no, offset, num_bytes);
        page->is_dirty_ = false;
    }
    
    // update page table
    auto it = page_table_.find(new_page_id);
    if (it != page_table_.end())
    	it->second = new_frame_id;
    
    // reset data of page, update page id
    page->reset_memory();
    page->id_ = new_page_id;
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim
 * page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
 
Page* BufferPoolManager::fetch_page(PageId page_id) {
    // Todo:
    //  1.     从page_table_中搜寻目标页
    //  1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    //  1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    //  2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    //  3.     调用disk_manager_的read_page读取目标页到frame
    //  4.     固定目标页，更新pin_count_
    //  5.     返回目标页

    std::scoped_lock lock{latch_};
    
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) { // page table history exists
    	// pin frame
    	replacer_->pin(it->second);
    	++pages_[it->second].pin_count_;
    	
    	return &pages_[it->second];
    }
    
    int target;
    if (find_victim_page(&target)){ // frame available
    
    	Page* page = &pages_[target];
    	if (page->is_dirty())
    		// write page back to disk
    		update_page(page, page_id, target);
    	else page->id_ = page_id;
    	
    	// read page to frame
    	getParams(page);
    	disk_manager_->read_page(fd, page_no, offset, num_bytes);
    	
    	// pin frame
    	page_table_.insert({page_id, target});
    	replacer_->pin(target);
    	++pages_[target].pin_count_;
    	return &pages_[target];
    }
    
    // no available frame
    return nullptr;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_

    std::scoped_lock lock{latch_};

	auto it = page_table_.find(page_id);
	if (it != page_table_.end()){
		//get page
		int frameId = page_table_[page_id];
		Page* page = &pages_[frameId];
		
		if (!page->pin_count_) return false;
		
		// pin_count > 0
		--page->pin_count_;
		if (!page->pin_count_) {
			// unpin
			replacer_->unpin(frameId);
		}
		page->is_dirty_ = is_dirty;
		
		return true;
	}
	
	// page not exists
	return false;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_

    std::scoped_lock lock{latch_};

    if (page_id.page_no == INVALID_PAGE_ID) {
        return false;
    }
	
	auto it = page_table_.find(page_id);
	if (it != page_table_.end()){
		int frameId = page_table_[page_id];
		Page* page = &pages_[frameId];
		
		getParams(page);
    	// write anyway
    	disk_manager_->write_page(fd, page_no, offset, num_bytes);
		
		return true;
	}
	
	// page table history nott found
	return false;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page

    std::scoped_lock lock{latch_};
    int frameId;
    if (find_victim_page(&frameId)) {
    	Page* page = &pages_[frameId];
    
       	getParams(page);
    	// write frame to disk
    	if (page->is_dirty())
    		disk_manager_->write_page(fd, page_no, offset, num_bytes);
    		
    		
    	// allocate a new page_id for fd
    	int newPageNo = disk_manager_->allocate_page(page_id->fd);
    	page_id->page_no = newPageNo;
    	page->id_ = *page_id;
    	page_table_[*page_id] = frameId;
    	

    	
    	// pin frame, update pin count
    	replacer_->pin(frameId);
    	++page->pin_count_;
    	page->reset_memory();
    	
    	return page;
    }
    
    // no available frame
    return nullptr;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true

    std::scoped_lock lock{latch_};
    
    // if page not in page table
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return true;

    int frameId = page_table_[page_id];
    Page* page = &pages_[frameId];
    
    // check pin count
    if (page->pin_count_) return false;
    
    // write page to disk
    getParams(page);
    disk_manager_->write_page(fd, page_no, offset, num_bytes);

	// delete page from table
    page_table_.erase(page_id);
    
    // reset page
    pages_[frameId].is_dirty_ = false;
    pages_[frameId].id_.fd = -1;
    pages_[frameId].id_.page_no = INVALID_PAGE_ID;
    pages_[frameId].reset_memory();

	// add to free list
    free_list_.emplace_back(frameId);

    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    for (auto it : page_table_)
    	flush_page(pages_[it.second].id_);
}
