#pragma once
#include "cachelib/navy/common/Device.h"

#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <thread>
#include <chrono>

#include <map>
#include <set>
#include <memory>
#include <mutex>
#include <tuple>
#include <functional>
#include <iostream>
#include <vector>
#include <queue>

#include <libzbd/zbd.h>
#include <fmt/core.h>

#include <folly/logging/xlog.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <folly/Executor.h>
#include <folly/init/Init.h>

#include <folly/synchronization/Baton.h>

#include "cachelib/navy/zone_cache/Zone.h"

#include <libexplain/fsync.h>
#include <libexplain/pwrite.h>
namespace facebook {
namespace cachelib {
namespace navy {

using DeviceInfo = zbd_info;

class ZonedDevice final : public Device {
 public:
  ZonedDevice(std::string devpath, uint64_t size,
             uint32_t ioAlignSize,
             std::shared_ptr<DeviceEncryptor> encryptor,
             uint32_t maxDeviceWriteSize, int flags = O_RDONLY, bool navyZnsRewrite=false, bool navyZnsGCReset=false): 
    devpath_(devpath), flags_(flags),
    Device{size, std::move(encryptor), ioAlignSize, maxDeviceWriteSize} {
    this->useRewrite = navyZnsRewrite;
    this->useReset = navyZnsGCReset;
    if (devpath_.back() == '_') {
      devpath = devpath.substr(0, devpath.size() - 1);
      zoneShift_ = 1000;
    }
    fd_ = zbd_open(devpath.c_str(), flags | O_DIRECT | O_LARGEFILE, &deviceInfo_);
    if (fd_ == -1) {
      throw std::runtime_error(fmt::format("open {} error", devpath));
    }
    const auto& deviceInfo = deviceInfo_;
    XLOGF(INFO, "open {} success. It has {} zones, each has 0x{:X} size, device aligns to 0x{:X}.", 
        devpath, deviceInfo.nr_zones, deviceInfo.zone_size, deviceInfo.pblock_size);
    initZoneInfo();
  };

  bool writeImpl(uint64_t offset, uint32_t size, const void* value) override {
    if (patchWrite) {
      auto bytesWritten = writeLargeData(value, size, offset);
      return bytesWritten == size;
    }
    auto bytesWritten = write(value, size, offset);
    return bytesWritten == size;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    auto bytesRead = read(value, size, offset);
    // XCHECK_EQ(bytesRead, size);
    return bytesRead == size;
  }

  void flushImpl() override { flush(); }

  ZonedDevice(const ZonedDevice&) = delete;
  ZonedDevice& operator=(const ZonedDevice&) = delete;

  ssize_t read(void *buf, uint64_t count, uint64_t offset);

  bool writeWihtHotness(uint64_t offset, Buffer buffer, uint64_t hotness) {
    const auto size = buffer.size();
    // XCHECK_LE(offset + buffer.size(), size_);
    uint8_t* data = reinterpret_cast<uint8_t*>(buffer.data());
    auto remainingSize = size;
    bool result = true;
    while (remainingSize > 0) {
      auto writeSize = remainingSize;
      auto timeBegin = getSteadyClock();
      auto bytesWritten = writeRegionWithHotness(data, writeSize, offset, hotness);
      result = bytesWritten == writeSize;
      // result = writeImpl(offset, writeSize, data);
      Device::writeLatencyEstimator_.trackValue(
          toMicros((getSteadyClock() - timeBegin)).count());

      if (result) {
        Device::bytesWritten_.add(writeSize);
      } else {
        // One part of the write failed so we abort the rest
        break;
      }
      offset += writeSize;
      data += writeSize;
      remainingSize -= writeSize;
    }
    if (!result) {
      Device::writeIOErrors_.inc();
    }
    return result;
  }

  ssize_t write(const void *buf, uint64_t count, uint64_t offset);

  ssize_t writeLargeData(const void *buf, uint64_t count, uint64_t offset);

  ssize_t writeRegionWithHotness(const void *buf, uint64_t count, uint64_t offset, uint64_t hotness);

  ssize_t writeRegionToZone(const void *buf, uint64_t count, uint64_t offset, std::shared_ptr<Zone> zone);

  ssize_t evict(void *buf, uint64_t count, uint64_t offset, size_t evictSize);

  Buffer evict(uint64_t offset, uint32_t size, size_t evictSize) {
    // XCHECK_LE(offset + size, size_);
    uint64_t readOffset =
        offset & ~(static_cast<uint64_t>(0x1000) - 1ul);
    uint64_t readPrefixSize =
        offset & (static_cast<uint64_t>(0x1000) - 1ul);
    auto readSize = getIOAlignedSize(readPrefixSize + size);
    auto buffer = makeIOBuffer(readSize);
    auto sz = evict(buffer.data(), readSize, readOffset, evictSize);
    if (sz != readSize) {
      return Buffer{};
    }
    buffer.trimStart(readPrefixSize);
    buffer.shrink(size);
    return buffer;
  }

  std::shared_ptr<Zone> openZoneForWrite();

  std::shared_ptr<Zone> openZoneForWriteWithHotness(uint64_t hotness);

  void flush();

  void setResetCallBack(std::function<bool(int64_t, folly::Baton<>&)> callback) {
    resetCallback_ = std::move(callback);
  }

  int fd() const { return fd_; }
  ~ZonedDevice() {
    stopGC.post();
    sleep(2);
    XLOG(INFO, "free up zns");
    exit(0);
  }
  
 private:  

  bool isHot(uint64_t hotness) {
    hotSum += hotness;
    hotCnt ++;
    hotAvg = (double) hotSum / (double) hotCnt;
    return hotness > hotAvg;
  }

  bool isZoneFull(int wi);
  std::tuple<CallResult, uint64_t> allocateWritingZone();
  std::tuple<CallResult, uint64_t> allocateWritingZoneWithHotness(uint64_t hotness);
  int pickOneWritingZone(bool almostFull=true);
  
  /* Zone Related */
  void initZoneInfo();

  uint64_t getZoneId(uint64_t offset) {
    // note: computing id should use zone_size rather capacity
    return offset / deviceInfo_.zone_size - zoneShift_;
  }

  std::shared_ptr<Zone>& getZone(uint64_t offset) {
    // note: computing id should use zone_size rather capacity
    return allZones_.at(getZoneId(offset));
  }

  /* Movement Algorithm Related */
  void startMovement();
  
  bool needReclaim();
  
  std::shared_ptr<Zone> findVictimLocked(bool must=false);

  ssize_t writeLocked(const void *buf, uint64_t count, std::shared_ptr<Zone> zone, uint64_t offset);

  bool cleanData(std::shared_ptr<Zone> &from, uint64_t offset, uint64_t endOffset, std::vector<std::shared_ptr<Zone>> to);
  
  /* Mapping Related */
  void insertMapping(uint64_t znsOffset, uint64_t size, uint64_t logOffset);

  void deleteMapping(const std::map<uint64_t, std::shared_ptr<DataBlockInfo>>::iterator &mappingIt);

  void changeMapping(std::shared_ptr<DataBlockInfo> &info, uint64_t newZnsOffset);

  /* Invalid Data Related */
  void markInvalidSpace(uint64_t offset, uint64_t size);

  void markInvalidSpaceLocked(uint64_t offset, uint64_t size);

  // use balaned tree to maintain the logical address
  // mapping store the **end** offset
  std::map<uint64_t, std::shared_ptr<DataBlockInfo>> mapping_;

  // hash map or tree map is same here
  // reverse mapping store the **start** offset
  // std::unordered_map<uint64_t, std::shared_ptr<DataBlockInfo>> reverseMapping_;

  std::mutex mappingLock_;
  std::mutex vectorLock_;
  
  int fd_;
  const int flags_;
  const std::string devpath_;
  DeviceInfo deviceInfo_;

  int maxWrtingZoneNumber{3};
  std::vector<std::shared_ptr<Zone>> writingZones_;
  std::vector<std::shared_ptr<Zone>> readingZones_;
  std::vector<std::shared_ptr<Zone>> allZones_;

  folly::Baton<> stopGC;
  // just for movement? 127 * 0x1000 bytes
  alignas(4096) char GCBuffer_[16777216];

  std::shared_ptr<Zone> tempZone_ = nullptr;
  std::shared_ptr<Zone> GCZone_ = nullptr;

  // config
  // when patchWrite = true, maxWriteSize should be 0.
  // =false is middle layer
  // step 2.
  bool patchWrite = false;
  // make sense only when using patchWrite
  bool useRewrite = false;

  bool useReset = true;
  std::function<bool(int64_t, folly::Baton<>&)> resetCallback_;

  bool statsGCMove = true;
  // 4k * dataReadForMoved = x bytes
  // dataReadForMoved += readBytes / 0x1000;
  uint64_t dataReadForMoved = 0;
  uint64_t dataWriteForMoved = 0;
  uint64_t newZoneMoved = 0;
  uint64_t zoneShift_ = 0;
  bool useStatsInLRU = false;
  double resetPercent = 1;

  double hotAvg = 0;
  uint64_t hotCnt = 0;
  uint64_t hotSum = 0;
};
}
}
}