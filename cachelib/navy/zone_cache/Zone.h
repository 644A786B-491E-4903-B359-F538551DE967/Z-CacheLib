#include <fcntl.h>
#include <unistd.h>
#include <tuple>

#include <folly/logging/xlog.h>
#include <libexplain/pwrite.h>
#include <set>
#include <bitset>
#include <libzbd/zbd.h>
namespace facebook {
namespace cachelib {
namespace navy {

using ZoneInvalidData = std::set<std::pair<uint64_t, uint64_t>>;

struct DataBlockInfo {
  uint64_t znsOffset;
  // logical offset
  uint64_t logOffset;
  uint64_t size;
  DataBlockInfo(uint64_t znsOffset, uint64_t size, uint64_t logOffset) :
    znsOffset(znsOffset), size(size), logOffset(logOffset) {}
};

// TO REMOVE
enum CallResult {
  SUCCESS,
  RETRY,
  DENY,
  FAILED
};

enum AppendStatus {
  DONE,
  NOSPACE,
  ERROR
};

struct AppendResult {
  AppendStatus status;
  uint64_t dataOffset;
};

class Zone {
 public:
  Zone(int fd, uint64_t zoneId, uint64_t start, uint64_t len, uint64_t size) : 
   fd_(fd), zoneId_(zoneId), start_(start), len_(len), size_(size), writePointer_(start) {
  }

  uint64_t getWritePointer() {
    // std::lock_guard<std::mutex> l(lock_);
    return writePointer_;
  }
  uint64_t getSize() const {return size_;}
  uint64_t getZoneId() const {return zoneId_;}
  uint64_t getStart() const {return start_;}
  
  bool canAllocate(uint64_t size) {
    return writePointer_ + size <= start_ + size_;
  }

  // TO REMOVE
  std::tuple<CallResult, uint64_t> allocate(uint64_t size) {
    // assert canAllocate(size)
    XDCHECK(canAllocate(size));
    if (writePointer_ == start_) {
      open();
    }
    writePointer_ += size;
    return {CallResult::SUCCESS, writePointer_};
  }

  // thread safe
  AppendResult append(const void *buf, uint64_t size, bool flashAfterWrite=false) {
    // std::lock_guard<std::mutex> l(lock_);
    // assert canAllocate(size)
    XCHECK_EQ((uint64_t) buf % 4096, 0);
    // XCHECK_EQ(alignof(buf), 4096);
    if (not canAllocate(size)) return {AppendStatus::NOSPACE, 0};

    // TODO: open cnt
    if (writePointer_ == start_) {
      open();
    }
    auto dataOffset = writePointer_;
    auto sz = pwrite(fd_, buf, size, dataOffset);

    if (sz != size) {
      XLOGF(ERR, "append 0x{:X} bytes to physic offset 0x{:X}, sz is {}.", size, writePointer_, sz);
      XLOGF(ERR, "{}", explain_pwrite(fd_, buf, size, dataOffset));
      throw std::ios_base::failure("append to zns error!");
    }

    writePointer_ += size;
    dataSize_ += size;

    if (flashAfterWrite) {
      auto err = fsync(fd_);
      if (err != 0) {
        XLOGF(ERR, "flush error after append 0x{:X} bytes to physic offset 0x{:X}, sz is {}.", size, writePointer_, sz);
        throw std::ios_base::failure("flush zns error!");
      }
    }

    return {AppendStatus::DONE, dataOffset};
  }

  bool open() {
    // std::lock_guard<std::mutex> l(lock_);
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_OPEN);
  }

  bool reset() {
    XLOG(DBG, "lock invalidlock in reset");
    std::lock_guard<std::mutex> lk(invalidlock_);
    // std::lock_guard<std::mutex> l(lock_);

    // invalidData.clear();
    bitmap.reset();

    // ensure logical is cleared
    logicalMapping_.clear();
    dataSize_ = 0;
    writePointer_ = start_;
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_RESET);
  }

  bool finish() {
    // std::lock_guard<std::mutex> l(lock_);
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_FINISH);
  }

  bool close() {
    // std::lock_guard<std::mutex> l(lock_);
    return doZoneCtrlOperation(zbd_zone_op::ZBD_OP_CLOSE);
  }

  uint32_t getFreeSizeLocked() const {
    // std::lock_guard<std::mutex> l(lock_);
    return size_ - writePointer_;
  }

  double getFreePercentLocked() const {
    // std::lock_guard<std::mutex> l(lock_);
    return (size_ - writePointer_) / (double) size_;
  }

  uint64_t getDataSize() const {
    // std::lock_guard<std::mutex> l(lock_);
    return dataSize_;
  }

  void markInvalid(uint64_t l, uint64_t r, bool changeDataSize=true) {
    // if (invalidlock_.try_lock()) {
    //   auto sz = r - l;
    //   if (sz > 0) {
    //     dataSize_ = dataSize_ - sz;
    //     XCHECK(dataSize_ >= 0);
    //   }
    //   invalidData.emplace(l, r);
    //   invalidlock_.unlock();
    // } else {
    //   // invalidData.emplace(l, r);
    //   XLOGF(ERR, "faild to mark invalid {} {}", l, r);
    // }
    // XLOG(INFO, "lock invalidlock in mark");
    std::lock_guard<std::mutex> m2lock(invalidlock_);
    auto sz = r - l;
    // when mark dead zone, not need to change dataSize_
    if (changeDataSize and sz > 0) {
      dataSize_ = dataSize_ - sz;
      XCHECK(dataSize_ >= 0);
    }
    int zl = (l - start_) / bitBlockSize;
    int zr = (r - start_) / bitBlockSize;
    for (int i = zl; i < zr; i++) {
      bitmap.set(i, true);
    }
    XLOGF(DBG, "current invalid {} cnt in zone {}", bitmap.count(), zoneId_);
    // XLOG(INFO, "unlock invalidlock in mark");
  }

  void markInvalidLocked(uint64_t l, uint64_t r) {
    // if (invalidlock_.try_lock()) {
    //   auto sz = r - l;
    //   if (sz > 0) {
    //     dataSize_ = dataSize_ - sz;
    //     XCHECK(dataSize_ >= 0);
    //   }
    //   invalidData.emplace(l, r);
    //   invalidlock_.unlock();
    // } else {
    //   // invalidData.emplace(l, r);
    //   XLOGF(ERR, "faild to mark invalid {} {}", l, r);
    // }
    // XLOG(INFO, "lock invalidlock in mark");
    // std::lock_guard<std::mutex> m2lock(invalidlock_);
    auto sz = r - l;
    if (sz > 0) {
      dataSize_ = dataSize_ - sz;
      XCHECK(dataSize_ >= 0);
    }
    int zl = (l - start_) / bitBlockSize;
    int zr = (r - start_) / bitBlockSize;
    for (int i = zl; i < zr; i++) {
      bitmap.set(i, true);
    }
    XLOGF(DBG, "current invalid {} cnt in zone {}", bitmap.count(), zoneId_);
    // XLOG(INFO, "unlock invalidlock in mark");
  }


  void foreachValidBlock(uint64_t offset, uint64_t endOffset,
    std::function<void()> preProcess,
    std::function<bool(std::shared_ptr<DataBlockInfo>&)> callback) {
    // int l = 0;
    int l = offset / bitBlockSize;
    int r = l;
    while (r < bitmap.size()) {
      // should lock map
      if (preProcess)
        preProcess();
      std::lock_guard<std::mutex> m2lock(invalidlock_);
      // valid
      while (r < bitmap.size() and bitmap[r] == false) r++;
      if (r - l > 0) {
        foreachValidSpace(start_ + l * bitBlockSize, start_ + r * bitBlockSize, callback);
      }
      // invalid
      while (r < bitmap.size() and bitmap[r] == true) r++;
      l = r;
    }
  }

  void foreachValidBlock(uint64_t offset, uint64_t endOffset, 
    std::function<bool(std::shared_ptr<DataBlockInfo>&)> callback) {
    std::lock_guard<std::mutex> m2lock(invalidlock_);
    // int l = 0;
    int l = offset / bitBlockSize;
    int r = l;
    while (r < bitmap.size()) {
      // should lock map
      // valid
      while (r < bitmap.size() and bitmap[r] == false) r++;
      if (r - l > 0) {
        foreachValidSpace(start_ + l * bitBlockSize, start_ + r * bitBlockSize, callback);
      }
      // invalid
      while (r < bitmap.size() and bitmap[r] == true) r++;
      l = r;
    }

    // // std::lock_guard<std::mutex> lk(lock_);
    // uint64_t l = offset;
    // // can be optimized by reducing number of calling foreach
    // // (1, 2) (4, 6) (6, 9)    12
    // // 
    // // assert the (l1, r1) < (l2, r2)
    // // asume endOffset is a valid address
    // // 
    // for (auto [r, il] : invalidData) {
    //   if (l < r) {
    //     if (r > endOffset) r = endOffset;
    //     foreachValidSpace(l, r, callback);
    //   }
    //   l = std::max(l, il);
    // }
    // if (l < endOffset) {
    //   foreachValidSpace(l, endOffset, callback);
    // }
  }

  void insertLogicalMapping(uint64_t k, std::shared_ptr<DataBlockInfo> v) {
    // std::lock_guard<std::mutex> lk(lock_);
    logicalMapping_.emplace(k, v);
  }

  void deleteLogicalMapping(uint64_t k) {
    // std::lock_guard<std::mutex> lk(lock_);
    logicalMapping_.erase(k);
  }

  mutable std::mutex readinglock_;
  mutable std::mutex invalidlock_;
  mutable std::mutex lock_;

  // 4k block
  std::bitset<0x43500> bitmap;
  uint64_t bitBlockSize = 0x1000;
  std::unordered_map<uint64_t, std::shared_ptr<DataBlockInfo>> logicalMapping_;

 private:

  // basic info
  const uint64_t zoneId_;
  const uint64_t start_;
  const uint64_t size_;
  const uint64_t len_;
  const int fd_;
  uint64_t writePointer_{0};

  uint64_t dataSize_{0};

  bool doZoneCtrlOperation(zbd_zone_op op) {
    auto res = zbd_zones_operation(fd_, op, start_, len_);
    return res;
  }

  // Zone > Space > Block
  void foreachValidSpace(uint64_t l, uint64_t r, std::function<bool(std::shared_ptr<DataBlockInfo>&)> callback) {
    // XLOGF(INFO, "l: 0x{:X} r: 0x{:X}", l, r);
    
    while (l < r) {
      // handle not find
      auto it = logicalMapping_.find(l);
      if (it == logicalMapping_.end()) {
        // auto lid = l % deviceInfo_.zone_size;
        XLOGF(ERR, "l: 0x{:X} r: 0x{:X} can't find l in reverseMapping_.", l, r);
        XLOGF(ERR, "zone is not full, so we can't find l=0x{:X} in reverseMapping_.", l, r);
        break;
        // TODO don't throw
        // throw std::logic_error("valid space should can be found in reverseMapping.");
      }
      auto &info = it->second;
      // [l, r) is valid
      auto ok = callback(info);
      if (!ok) {
        // throw
        XLOGF(ERR, "callback return false when handle block zns: 0x{:X} log: 0x{:X} sz: {}.", info->znsOffset, info->logOffset, info->size);
        throw std::logic_error("callback return false");
      }
      l += info->size;
    }
  }

};

}
}
}