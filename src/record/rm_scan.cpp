/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
 
 // get the first place used 
 #define isStored(byteN, bitN)	\
 	((pageHandle.bitmap[byteN] >> (7 - bitN)) & 1)
 // calc slot position
 #define slotGet(byteN, bitN)	\
 	(byteN * 8 + bitN)
 // get position through slot
 #define positionGet(slotNo)	\
 	int byteN = slotNo / 8;	\
 	int bitN = slotNo - byteN;
 // fetch page handle by pageNo
 #define fetchPageHandle(pageNo)	\
 	RmPageHandle pageHandle = file_handle_->fetch_page_handle(pageNo);
 // set rid_
 #define setRid(pageNo, byteN, bitN)	\
 	rid_ = {pageNo, slotGet(byteN, bitN)};
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）

    int numPages = file_handle_->file_hdr_.num_pages;

    for (int pageNo = 1; pageNo < numPages; pageNo++) { // tranverse all pages
        fetchPageHandle(pageNo);
        int bitmapSize = file_handle_->file_hdr_.bitmap_size;
        for (int byteN = 0; byteN < bitmapSize; byteN++)
        	for (int bitN = 0; bitN < 8; bitN++) // judge by bit to find
        		if (isStored(byteN, bitN)){
        			setRid(pageNo, byteN, bitN);
        			return;
        		}
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
 
 #define PAGE_END	-123
 #define SLOT_END	-123
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    int numPages = file_handle_->file_hdr_.num_pages;
    int bitmapSize = file_handle_->file_hdr_.bitmap_size;
    int slotNo = rid_.slot_no;
    int pageno = rid_.page_no;
    positionGet(slotNo);
    
    bool theFirst = true;
    for (int pageNo = pageno; pageNo < numPages; pageNo++) {
    	fetchPageHandle(pageNo);
    	for (int ByteN = theFirst ? byteN : 0; ByteN < bitmapSize; ByteN++) 
    		for (int BitN = theFirst ? bitN+1 : 0; BitN < 8; BitN++) {
    			theFirst = false;
    			if (isStored(ByteN, BitN)) {
    				setRid(pageNo, ByteN, BitN);
    				return;
    			}
    		}
    }
    
    // return end
    rid_ = {PAGE_END, SLOT_END};
    return;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end()
    const {
    // Todo: 修改返回值

    if (rid_.page_no == PAGE_END && rid_.slot_no == SLOT_END) {
        return true;
    }

    return false;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const { return rid_; }
