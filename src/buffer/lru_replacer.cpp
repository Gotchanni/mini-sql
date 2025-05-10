#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : cache(num_pages, vic_.end()) {
  max_page = num_pages;
}

LRUReplacer::~LRUReplacer() = default;
/*
替换（即删除）与所有被跟踪的页相比最近最少被访问的页，
将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中
输出并返回true，如果当前没有可以替换的元素则返回false
*/
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 如果没有可以替换的元素，返回false
  if (vic_.empty()!=0) {
    return false;
  }
  // 如果有可以替换的元素,将最近最少使用的元素（即链表的最后一个元素）替换掉,将其页帧号存储在输出参数frame_id中
  // 并将其从链表中删除
  else {
    auto it = prev(vic_.end());
    *frame_id = *it;
    cache[*frame_id] = vic_.end();
    vic_.erase(it);
    return true;
  }
}
/*
将数据页固定使之不能被Replacer替换，即从lru_list_中移除该数据页对应的页帧。
Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用；
*/
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto it = cache[frame_id];
  if (it != vic_.end()) {
    vic_.erase(it);
    cache[frame_id] = vic_.end();
  }
}
/*
将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉。
Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，
使页帧对应的数据页能够在必要时被替换；
*/
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 如果页帧号不合法，直接返回
  if (vic_.size() >= max_page ) {
    return;
  }
  // 如果页帧号已经存在于链表中，直接返回
  else if (cache[frame_id] != vic_.end()){
    return ;
  }
  // 如果页帧号不存在于链表中，将其添加到链表的头部
  else{
    vic_.push_front(frame_id);
    cache[frame_id] = vic_.begin();
  }

}

size_t LRUReplacer::Size() { return vic_.size(); }