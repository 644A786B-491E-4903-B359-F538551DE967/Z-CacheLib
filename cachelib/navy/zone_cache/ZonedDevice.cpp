#include "cachelib/navy/zone_cache/ZonedDevice.h"
#include <cstdint>
#include <cstring>
namespace facebook {
namespace cachelib {
namespace navy {

/* Zone Related */
void ZonedDevice::initZoneInfo() {
  // int flags = O_RDWR;
  zbd_get_info(fd_, &deviceInfo_);
  if (devpath_.back() == '_') {
    // + 11
    deviceInfo_.nr_zones = 75;
    // max open zone is 14
    maxWrtingZoneNumber = 6;
  } else {
      // TODO for test
    // 932 + 14
    // 94 + 14
    // + 5
    // deviceInfo_.nr_zones = 110;
    // 200G
    // step 1.
    // deviceInfo_.nr_zones = 204;
    // 400G
    // deviceInfo_.nr_zones = 392;
    // 600G
    // deviceInfo_.nr_zones = 580;
    // 800G
    // deviceInfo_.nr_zones = 768;
    // 10000G
    // deviceInfo_.nr_zones = 956;
    // max open zone is 14
    maxWrtingZoneNumber = 12;
  }
  auto nr_zones = deviceInfo_.nr_zones;
  XLOGF(WARNING, "using test nr_zones={} and maxWrtingZoneNumber={}", nr_zones, maxWrtingZoneNumber);

  std::vector<zbd_zone> zones;
  zones.resize(deviceInfo_.nr_zones);
  // allZones_.resize(deviceInfo_.nr_zones);
  // invalidData_.resize(deviceInfo_.nr_zones);

  unsigned int readSize = zones.size();
  uint64_t zeroZone = 0 + zoneShift_;
  
  zbd_reset_zones(fd_, 0, 0);
  zbd_report_zones(fd_, zeroZone * deviceInfo_.zone_size, 0, zbd_report_option::ZBD_RO_ALL, zones.data(), &readSize);
  // flush();
  const auto zone0 = zones.at(zeroZone - zoneShift_);
  tempZone_ = std::make_shared<Zone>(fd_, zeroZone, zone0.start, zone0.len, zone0.capacity);
  tempZone_->reset();
  allZones_.push_back(tempZone_);

  uint64_t firstZone = 1 + zoneShift_;
  uint64_t lastZone = zones.size() - 1 + zoneShift_;
  for (uint64_t i = firstZone; i < lastZone; i++) {
    const auto &zone = zones.at(i - zoneShift_);
    XCHECK_EQ(i, zone.start / zone.len);
    auto p = std::make_shared<Zone>(fd_, i, zone.start, zone.len, zone.capacity);
    // p->reset();
    writingZones_.push_back(p);
    allZones_.push_back(p);
  }

  const auto &gcZone = zones.at(lastZone - zoneShift_);
  GCZone_ = std::make_shared<Zone>(fd_, lastZone, gcZone.start, gcZone.len, gcZone.capacity);
  // GCZone_->reset();
  allZones_.push_back(GCZone_);

  auto backGC = true;
  if (backGC) {
    folly::getGlobalIOExecutor()->add([=]() {
      startMovement();
    });
  }
}

ssize_t ZonedDevice::evict(void *buf, uint64_t count, uint64_t offset, size_t evictSize) {
  XLOGF(DBG, "begin evicting 0x{:X} bytes using logical offset 0x{:X}.", count, offset);
  ssize_t readBytes = 0;
  int64_t leftBytes = count;
  int64_t leftEvictSize = evictSize;
  // only insert the end of file map.insert(offset + size, info);
  while (leftBytes > 0) {
    // std::lock_guard<std::mutex> l(mappingLock_);
    mappingLock_.lock();
    // x 100  x 400 x 500
    // auto it = mapping_.find(offset + count);
    auto it = mapping_.lower_bound(offset + 1);
    if (it == mapping_.end()) {
      mappingLock_.unlock();
      // there is no value larger than offset
      break;
    }

    XDCHECK_GE(offset, it->second->logOffset);
    auto shift = offset - it->second->logOffset;
    auto info = *it->second;
    auto z = getZone(info.znsOffset + shift);
    // z->incActiveReaders();
    // lock when reading to prevent moving
    // z->lock_.lock();
    auto toReadBytes = std::min((int64_t) (info.size - shift), leftBytes);
    
    auto toEvictBytes = std::min((int64_t) (info.size - shift), leftEvictSize);
    // z->markInvalid();
    z->markInvalid(info.znsOffset + shift, info.znsOffset + shift + toEvictBytes);
    deleteMapping(it);

    std::lock_guard<std::mutex> lck(z->readinglock_);
    mappingLock_.unlock();

    // read 0x40000 bytes from physic offset 0x712434D5000, using logical offset 0x1EE2D5000.
    XLOGF(DBG, "evict 0x{:X} bytes read 0x{:X} bytes from physic offset 0x{:X}, using logical offset 0x{:X}.", toEvictBytes, toReadBytes, info.znsOffset + shift, offset);
    // TODO store offset and znsOffset to enable parallel reading
    auto sz = pread(fd_, (uint8_t *)buf + readBytes, toReadBytes, info.znsOffset + shift);
    XLOGF(DBG, "physic offset 0x{:X} example: {} {} {}", info.znsOffset + shift, ((uint8_t *)buf + readBytes)[0], 
        ((uint8_t *)buf + readBytes)[1], ((uint8_t *)buf + readBytes)[2]);
    // z->decActiveReaders();

    // z->cv_.notify_all();
    // z->lock_.unlock();
    if (sz == -1) {
      throw std::ios_base::failure("read from zns error!");
      return sz;
    }
    offset += sz;
    leftBytes -= sz;
    readBytes += sz;
  }
  XDCHECK_EQ(readBytes, count);
  return readBytes;
}

ssize_t ZonedDevice::read(void *buf, uint64_t count, uint64_t offset) {
  XLOGF(DBG, "begin reading 0x{:X} bytes using logical offset 0x{:X}.", count, offset);
  ssize_t readBytes = 0;
  int64_t leftBytes = count;
  // only insert the end of file map.insert(offset + size, info);
  while (leftBytes > 0) {
    // std::lock_guard<std::mutex> l(mappingLock_);
    mappingLock_.lock();
    // x 100  x 400 x 500
    // auto it = mapping_.find(offset + count);
    auto it = mapping_.lower_bound(offset + 1);
    if (it == mapping_.end()) {
      mappingLock_.unlock();
      // there is no value larger than offset
      break;
    }
    
    if (not (it->second->logOffset <= offset and offset < it->second->logOffset + it->second->size)) {
      mappingLock_.unlock();
      break;
      XLOGF(ERR, "offset not right, 0x{:X}", offset);
      throw std::logic_error("offset not right");
    }
    XCHECK_GE(offset, it->second->logOffset);
    XCHECK_LT(offset, it->second->logOffset + it->second->size);
    auto shift = offset - it->second->logOffset;
    auto info = *it->second;
    auto z = getZone(info.znsOffset + shift);

    // z->incActiveReaders();

    // lock when reading to prevent moving
    // z->lock_.lock();

    std::lock_guard<std::mutex> lck(z->readinglock_);
    mappingLock_.unlock();

    auto toReadBytes = std::min((int64_t) (info.size - shift), leftBytes);
    // read 0x40000 bytes from physic offset 0x712434D5000, using logical offset 0x1EE2D5000.
    XLOGF(DBG, "read 0x{:X} bytes from physic offset 0x{:X}, using logical offset 0x{:X}.", toReadBytes, info.znsOffset + shift, offset);
    // TODO store offset and znsOffset to enable parallel reading
    auto sz = pread(fd_, (uint8_t *)buf + readBytes, toReadBytes, info.znsOffset + shift);
    XLOGF(DBG, "physic offset 0x{:X} example: {} {} {}", info.znsOffset + shift, ((uint8_t *)buf + readBytes)[0], 
        ((uint8_t *)buf + readBytes)[1], ((uint8_t *)buf + readBytes)[2]);
    // z->decActiveReaders();
    // z->cv_.notify_all();
    // z->lock_.unlock();
    if (sz == -1) {
      throw std::ios_base::failure("read from zns error!");
      return sz;
    }
    offset += sz;
    leftBytes -= sz;
    readBytes += sz;
  }
  if (readBytes != count and readBytes == 0) {
    // fake read
    memset(buf, '\0', count);
    readBytes += count;
  }
  XCHECK_EQ(readBytes, count);
  return readBytes;
}

// TODO
int ZonedDevice::pickOneWritingZone(bool almostFull) {
  auto maxOpen = maxWrtingZoneNumber - 2;
  if (writingZones_.size() > 0) {
    if (not patchWrite) {
      return writingZones_.size() - 1;
    }
    if (writingZones_.size() < maxOpen) {
      return rand() % writingZones_.size();
    } else {
      auto i = rand() % maxOpen;
      XCHECK_LT(i, writingZones_.size());
      return writingZones_.size() - 1 - i;
    }
  } else {
    return -1;
    // throw std::logic_error("no writing zone");
  }
}

std::tuple<CallResult, uint64_t> ZonedDevice::allocateWritingZone() {
  auto almostFull = !(writingZones_.size() > maxWrtingZoneNumber);
  auto wi = pickOneWritingZone(almostFull);
  if (wi == -1) {
    return {CallResult::RETRY, wi};
  }
  return {CallResult::SUCCESS, wi};
}

std::tuple<CallResult, uint64_t> ZonedDevice::allocateWritingZoneWithHotness(uint64_t hotness) {
  // auto almostFull = !(writingZones_.size() > maxWrtingZoneNumber);
  auto maxOpen = maxWrtingZoneNumber - 2;
  if (writingZones_.size() > 0) {
    if (writingZones_.size() < maxOpen) {
      auto wi = rand() % writingZones_.size();
      if (isHot(hotness)) {
        wi = rand() % writingZones_.size() / 2;
      }
      return {CallResult::SUCCESS, wi};
    } else {
      auto i = rand() % maxOpen;
      XCHECK_LT(i, writingZones_.size());
      return {CallResult::SUCCESS, writingZones_.size() - 1 - i};
    }
  } else {
    return {CallResult::RETRY, -1};
    // throw std::logic_error("no writing zone");
  }
}

bool ZonedDevice::isZoneFull(int wi) {
  // TODO ensure there is no writing or reading in zone wi
  // auto almostFull = !(writingZones_.size() > maxWrtingZoneNumber);
  auto &p = writingZones_.at(wi);
  auto wp = p->getWritePointer();

  if (wp + 0x1000000 > p->getStart() + p->getSize()) {
    XLOGF(DBG, "change full writing zones {} to reading zones. wp is 0x{:X}", p->getZoneId(), wp);
    // auto c = pickOneReadingZone();
    p->markInvalid(p->getWritePointer(), p->getStart() + p->getSize(), false);
    XLOGF(DBG, "dead zone 0x{:X} 0x{:X}", p->getWritePointer(), p->getStart() + p->getSize());
    p->finish();
    // p->close();
    readingZones_.push_back(std::move(p));
    writingZones_.erase(writingZones_.begin() + wi);
    return true;
  } else {
    return false;
  }
}

ssize_t ZonedDevice::writeRegionToZone(const void *buf, uint64_t count, uint64_t offset, std::shared_ptr<Zone> zone) {
// XLOGF(INFO, "zone is locked {}", zone->getZoneId());
  uint64_t maxWriteSize = 0x1000 * 64;
  const uint8_t* data = reinterpret_cast<const uint8_t*>(buf);
  auto remainingSize = count;
  auto logicalAddress = offset;
  maxWriteSize = (maxWriteSize == 0) ? remainingSize : maxWriteSize;

  bool ok = true;
  int64_t dataOffset = -1;

  if (remainingSize == 0) {
    throw std::logic_error("can't write zero size data");
  }

  while (remainingSize > 0) {
    auto writeSize = std::min<size_t>(maxWriteSize, remainingSize);
    XCHECK_EQ(offset % 0x1000, 0ul);
    XCHECK_EQ(writeSize % 0x1000, 0ul);
    // lock zone
    auto appRes = zone->append(data, writeSize);
    if (appRes.status == AppendStatus::DONE) {
      if (dataOffset == -1) {
        dataOffset = appRes.dataOffset;
      }
      offset += writeSize;
      data += writeSize;
      remainingSize -= writeSize;
    } else {
      ok = false;
      break;
    }
  }
  zone->lock_.unlock();
  // XLOGF(INFO, "zone is unlocked {}", zone->getZoneId());
  if (ok) {
    mappingLock_.lock();
    if (useRewrite) {
      markInvalidSpace(logicalAddress, count);
    }
    insertMapping(dataOffset, count, logicalAddress);
    mappingLock_.unlock();
    return count;
  } else {
    XLOGF(ERR, "write {} bytes data to zone {} failed.", count, zone->getZoneId());
    return 0;
  }
}

ssize_t ZonedDevice::writeLargeData(const void *buf, uint64_t count, uint64_t offset) {
  // XLOGF(INFO, "begin write 0x{:X} bytes using logical offset 0x{:X}.", count, offset);
  int cnt = 0;
  reopen:
  auto zone = openZoneForWrite();
  if (zone == nullptr) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    XLOG(DBG, "waiting");
    cnt += 1;
    if (cnt > 2) {
      return -1;
    }
    goto reopen;
    // return -1;
  }
  return writeRegionToZone(buf, count, offset, zone);
}

ssize_t ZonedDevice::writeRegionWithHotness(const void *buf, uint64_t count, uint64_t offset, uint64_t hotness) {
  // XLOGF(INFO, "begin write 0x{:X} bytes using logical offset 0x{:X}.", count, offset);
  int cnt = 0;
  reopen:
  // TODO here
  auto zone = openZoneForWriteWithHotness(hotness);
  if (zone == nullptr) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    XLOG(DBG, "waiting");
    cnt += 1;
    if (cnt > 2) {
      return -1;
    }
    goto reopen;
    // return -1;
  }
  return writeRegionToZone(buf, count, offset, zone);
}

std::shared_ptr<Zone> ZonedDevice::openZoneForWrite() {
  auto moved = true;
  std::shared_ptr<Zone> zone = nullptr;
  // auto zone = writingZones_.at(wi);
  // lock vector, zone
  vectorLock_.lock();
  // XLOG(INFO, "locked");
  int cnt = 0;
  while (moved) {
    auto [res, wi] = allocateWritingZone();
    if (res == CallResult::RETRY) {
      vectorLock_.unlock();
      return nullptr;
    }
    zone = writingZones_.at(wi);
    // XLOGF(INFO, "wait zone lock {}", zone->getZoneId());
    zone->lock_.lock();
    moved = isZoneFull(wi);
    if (moved) {
      zone->lock_.unlock();
      // XLOGF(INFO, "zone unlock {}", zone->getZoneId());
      cnt ++;
      if (cnt > 10) {
        throw std::logic_error("too many recheck");
      }
      // XLOG(INFO, "recheck");
    }
  }
  // XLOGF(INFO, "zone {} is locked in write.", zone->getZoneId());

  vectorLock_.unlock();
  return zone;
}

std::shared_ptr<Zone> ZonedDevice::openZoneForWriteWithHotness(uint64_t hotness) {
  auto moved = true;
  std::shared_ptr<Zone> zone = nullptr;
  // auto zone = writingZones_.at(wi);
  // lock vector, zone
  vectorLock_.lock();
  // XLOG(INFO, "locked");
  int cnt = 0;
  while (moved) {
    auto [res, wi] = allocateWritingZoneWithHotness(hotness);
    if (res == CallResult::RETRY) {
      vectorLock_.unlock();
      return nullptr;
    }
    zone = writingZones_.at(wi);
    // XLOGF(INFO, "wait zone lock {}", zone->getZoneId());
    zone->lock_.lock();
    moved = isZoneFull(wi);
    if (moved) {
      zone->lock_.unlock();
      // XLOGF(INFO, "zone unlock {}", zone->getZoneId());
      cnt ++;
      if (cnt > 10) {
        throw std::logic_error("too many recheck");
      }
      // XLOG(INFO, "recheck");
    }
  }
  // XLOGF(INFO, "zone {} is locked in write.", zone->getZoneId());

  vectorLock_.unlock();
  return zone;
}

ssize_t ZonedDevice::write(const void *buf, uint64_t count, uint64_t offset) {
  // let top layer to split the size of big file
  // here count is limited with maxWriteSize
  // buf should aligned 4k
  reopen:
  auto zone = openZoneForWrite();
  if (zone == nullptr) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    goto reopen;
    // return -1;
  }
  // lock zone
  auto appRes = zone->append(buf, count);
  zone->lock_.unlock();
  // XLOGF(INFO, "write at zone {} with logical address 0x{:X}.", zone->getZoneId(), offset);
  
  // lock mapping
  if (appRes.status == AppendStatus::DONE) {
    // XLOG(INFO, "lock mapping in write");
    mappingLock_.lock();
    // XLOGF(INFO, "zone {} do mark invalid.", zone->getZoneId());
    markInvalidSpace(offset, count);
    if (count == 0) {
      XLOG(ERR, "ERR");
      throw std::logic_error("ERR");
    }
    insertMapping(appRes.dataOffset, count, offset);
    mappingLock_.unlock();
    // XLOG(INFO, "unlock mapping in write");
    return count;
  } else {
    XLOG(ERR, "no space for appending.");
    return 0;
  }
  
}

ssize_t ZonedDevice::writeLocked(const void *buf, uint64_t count, std::shared_ptr<Zone> zone, uint64_t offset) {
  uint64_t maxWriteSize = 0x1000 * 64;
  const uint8_t* data = reinterpret_cast<const uint8_t*>(buf);
  auto remainingSize = count;
  auto logicalAddress = offset;
  maxWriteSize = (maxWriteSize == 0) ? remainingSize : maxWriteSize;
  
  // auto zone = openZoneForWrite();
  bool ok = true;
  int64_t dataOffset = -1;

  if (remainingSize == 0) {
    throw std::logic_error("can't write zero size data");
  }

  while (remainingSize > 0) {
    auto writeSize = std::min<size_t>(maxWriteSize, remainingSize);
    XCHECK_EQ(offset % 0x1000, 0ul);
    XCHECK_EQ(writeSize % 0x1000, 0ul);
    // lock zone
    auto appRes = zone->append(data, writeSize);
    if (appRes.status == AppendStatus::DONE) {
      if (dataOffset == -1) {
        dataOffset = appRes.dataOffset;
      }
      offset += writeSize;
      data += writeSize;
      remainingSize -= writeSize;
    } else {
      ok = false;
      break;
    }
  }
  zone->lock_.unlock();
  // XLOGF(INFO, "write at zone {} with logical address 0x{:X}.", zone->getZoneId(), offset);
  if (ok) {
    return count;
  } else {
    XLOG(ERR, "no space for appending.");
    return 0;
  }
}


void ZonedDevice::flush() {
  std::lock_guard<std::mutex> l(mappingLock_);
  auto res = fsync(fd_);
  if (res == -1) {
    throw std::ios_base::failure("flash to zns device error.");
  }
}

/* Movement Algorithm Related */

bool ZonedDevice::cleanData(std::shared_ptr<Zone> &from, uint64_t offset, uint64_t endOffset, std::vector<std::shared_ptr<Zone>> to) {
  // XLOGF(INFO, "moving {} 0x{:X}-0x{:X} data to {} wp=0x{:X}.", from->getZoneId(), *offsetPtr, endOffset, to->getZoneId() ,to->getWritePointer());
  // move [offset, size) data from `from` to `to`, discard invalid data in the range
  auto zid = from->getZoneId();
  XCHECK_EQ(to.size(), 2);
  std::reverse(to.begin(), to.end());

  auto toMoveBytes = from->getDataSize();
  uint64_t curMovedBytes = 0;
  uint64_t curResetBytes = 0;

  

  auto callback2 = [&](std::shared_ptr<DataBlockInfo> &info) -> bool {
  // foreachValidBlock(invalidData_.at(zid), offset, endOffset, [&](std::shared_ptr<DataBlockInfo> &info) -> bool {
    // XCHECK(canAllocate(info->size));
    while (not to.back()->canAllocate(info->size)) {
      auto z = to.back();
      z->markInvalid(z->getWritePointer(), z->getStart() + z->getSize(), false);
      z->finish();
      to.pop_back();
      XCHECK_EQ(to.size(), 1);
    }
    auto zone = to.back();
    
    auto readBytes = pread(fd_, GCBuffer_, info->size, info->znsOffset);

    if (statsGCMove) {
      dataReadForMoved += readBytes / 0x1000;
    }

    if (readBytes == info->size) {
      // allocate

      // TODO lock when zone is moving
      auto binfo = *info;
      // auto predDataOffset = to.back()->getWritePointer();
      // changeMapping(info, predDataOffset);
      markInvalidSpaceLocked(info->logOffset, info->size);
      insertMapping(zone->getWritePointer(), binfo.size, binfo.logOffset);
      std::lock_guard<std::mutex> lck(zone->readinglock_);
      mappingLock_.unlock();
      from->invalidlock_.unlock();
      XLOG(DBG, "lock invalidlock");

      if (statsGCMove) {
        dataWriteForMoved += binfo.size / 0x1000;
      }
      curMovedBytes += binfo.size;
      auto sz = writeLocked(GCBuffer_, binfo.size, zone, binfo.logOffset);
      // auto appendRes = to.back()->append(GCBuffer_, info->size);
      if (sz == readBytes) {
        XLOGF(DBG, "data move at logical 0x{:X} is done", binfo.logOffset);
        return true;
      } else {
        XLOGF(ERR, "move err get {} expected: {}", sz, binfo.size);
        throw std::logic_error("move error.");
      }
    } else {
      XLOGF(ERR, "can't read {} bytes at 0x{:X} for move", info->size, info->znsOffset);
      throw std::ios_base::failure("can read when move data");
    }
  };

  auto callback = [&](std::shared_ptr<DataBlockInfo> &info) -> bool {
    auto binfo = *info;
    folly::Baton<> done;
    if (bottomUpEvicion) {
      bool keep = resetCallback_(binfo.logOffset, done);
      if (keep) {
        return callback2(info);
        // return true;
      }
    }
    mappingLock_.unlock();
    from->invalidlock_.unlock();
    XLOG(DBG, "unlock invalidlock");
    // while (not done.ready()) {
    //   XLOGF(ERR, "waiting for evict 0x{:X}", binfo.logOffset);
    //   std::this_thread::sleep_for(std::chrono::milliseconds(80));
    // }
    done.wait();
    return true;
  };
     // int l = 0;
  XCHECK_LE(from->getStart(), offset);


  uint64_t l = (offset - from->getStart()) / from->bitBlockSize;
  // int64_t r = l;
  auto cnt = 0;
  while (l < from->bitmap.size()) {
    // should lock map
    // XLOG(INFO, "lock mapping");
    mappingLock_.lock();

    // XLOG(INFO, "lock invalidlock");
    // from->invalidlock_.lock();
    // std::lock_guard<std::mutex> m2lock(from->invalidlock_);

    from->invalidlock_.lock();
    // find the first zero
    while (l < from->bitmap.size() and from->bitmap[l] == true) l++;
    if (l == from->bitmap.size()) {
      mappingLock_.unlock();
      from->invalidlock_.unlock();
      XLOG(DBG, "unlock invalidlock");
      break;
    }
    // find l in logical mapping
    int64_t ll = from->getStart() + l * from->bitBlockSize;
    auto it = from->logicalMapping_.find(ll);
    if (it == from->logicalMapping_.end()) {
      XLOGF(ERR, "l: 0x{:X} can't find l in reverseMapping_.", ll);
      XLOGF(ERR, "zone is not full, so we can't find l=0x{:X} in reverseMapping_.", ll);
      mappingLock_.unlock();
      from->invalidlock_.unlock();
      break;
      // throw std::logic_error("can't find logical address");
    }
    auto &info = it->second;
    
    auto infoSize = info->size;
    if (infoSize == 0) {
      XLOG(ERR, "ERR");
      throw std::logic_error("info size is zero?");
    }
    // real process
    auto ok = false;
    auto p = (double)curResetBytes / (double) (toMoveBytes + 1);
    if (useReset and p < resetPercent) {
      // reset
      curResetBytes += info->size;
      ok = callback(info);
    } else {
      // move
      ok = callback2(info);
    }
    // int r = rand() % 100;
    // if (useReset and p > 1 - resetPercent) {
    //   ok = callback(info);
    // } else {
    //   ok = callback2(info);
    // }
    if (!ok) {
      XLOGF(ERR, "callback return false when handle block zns: 0x{:X} log: 0x{:X} sz: {}.", info->znsOffset, info->logOffset, info->size);
      throw std::logic_error("callback return false");
    }

    // update l and cnt
    cnt += infoSize / from->bitBlockSize;
    l += infoSize / from->bitBlockSize;
  }

  // XCHECK_EQ(cnt, from->bitmap.size() - from->bitmap.count());

  XCHECK(to.size() == 2 or to.size() == 1);
  return to.size() == 1;
}

bool ZonedDevice::needReclaim() {
  // std::lock_guard<std::mutex> l(vectorLock_);
  // when writing zone is less
  auto isNeedReclaim = writingZones_.size() < maxWrtingZoneNumber;
  return isNeedReclaim;
}

std::shared_ptr<Zone> ZonedDevice::findVictimLocked(bool must) {
  bool withLock = true;

  std::shared_ptr<Zone> victim = nullptr;
  int victimIdx = -1;

  std::shared_ptr<Zone> minptr = nullptr;
  int mini = -1;
  int64_t minv = INT64_MAX;

  for (int i = 0; i < readingZones_.size(); i++) {
    auto &zone = readingZones_.at(i);
    if (zone->lock_.try_lock()) {
      auto validPercent = zone->getDataSize() / (double) zone->getSize();
      // XLOGF(INFO, "zone data size {}M", zone->getDataSize() / 1024 /1024);
      if (validPercent < 0.5) {
        victim = zone;
        victimIdx = i;
        // XLOGF(INFO, "zone {} is locked.", zone->getZoneId());
        // zone->lock_.unlock();
        break;
      }
      if (must and minv > zone->getDataSize()) {
        minv = zone->getDataSize();
        mini = i;
        minptr = zone;
      }
      zone->lock_.unlock();
    }
  }
  if (victimIdx != -1) {
    readingZones_.erase(readingZones_.begin() + victimIdx);
    return victim;
  } else if (must and mini != -1) {
    XLOGF(DBG, "locking min zone {} with must flag", minptr->getZoneId());
    minptr->lock_.lock();
    readingZones_.erase(readingZones_.begin() + mini);
    return minptr;
  }
  return nullptr;
}

void ZonedDevice::startMovement() {
  while (not stopGC.ready()) {
    // sleep(1);
    // 200ms
    bool justSleep = false;

    if (justSleep) {
      sleep(1);
      continue;
    }
    // auto pre = std::chrono::system_clock::now();
    // auto now = std::chrono::system_clock::now();
    // auto s = now - pre;
    // XLOGF(INFO, "lock time: {}", s.count());
    
    // lock vector
    vectorLock_.lock();

    if (not needReclaim()) {
      vectorLock_.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      continue;
    }
    XLOGF(DBG, "current have {} writing zones.", writingZones_.size());
    // lock vector, zone
    // XLOG(INFO, "test 1");
    auto must = writingZones_.size() < maxWrtingZoneNumber;

    auto victim = findVictimLocked(must);
    vectorLock_.unlock();
    if (victim == nullptr) {
      continue;
    }
    // XLOG(INFO, "test 2");
    // lock zone

    // victim->lock_.unlock();
    XLOGF(INFO, "find victim {} data size {} M {} must {}.", victim->getZoneId(), victim->getDataSize() / 1024 / 1024, victim->getDataSize(), must ? "true" : "false");
    auto victimDataSize = victim->getDataSize();

    // when mapping and invalid is acquired before clean data.
    // victimDataSize is not right
    GCZone_->lock_.lock();
    tempZone_->lock_.lock();

    auto preSize = GCZone_->getWritePointer();
    
    // if victim can make temp full
    // lock zone, mapping

    auto useTemp = cleanData(victim, victim->getStart() + 0, victim->getStart() + victim->getSize(), {GCZone_, tempZone_});
    
    uint64_t gcWriteBytes = GCZone_->getWritePointer() - preSize;
    uint64_t tempWriteBytes = tempZone_->getWritePointer() - tempZone_->getStart();

    // moveData(victim, victim->getStart() + 0, victim->getStart() + victim->getSize(), tempZone_);
    if (victim->getDataSize() != 0) {
      XLOGF(ERR, "current victim {} data size {} ", victim->getZoneId(), victim->getDataSize());
      XLOGF(ERR, "victim {} data size {} ", victim->getZoneId(), victimDataSize);
      XLOGF(ERR, "gczone {} data size {} , pre {}", GCZone_->getZoneId(), GCZone_->getWritePointer(), preSize);
      XLOGF(ERR, "tempzone {} data size {} ", tempZone_->getZoneId(), tempZone_->getWritePointer());
      // throw std::logic_error("movement error");
    }
    // XCHECK_EQ(victimDataSize, gcWriteBytes + tempWriteBytes);
    
    // std::unique_lock<std::mutex> lck(victim->readinglock_);
    // victim->cv_.wait(lck, [=]() {return victim->getActiveReaders() == 0;});
    
    victim->reset();
    // make victim zone empty

    if (statsGCMove) {
      newZoneMoved++;
      std::cout << "STATS: \t" << dataWriteForMoved / 256 << "M data have be written for moving, avarage "  <<  dataWriteForMoved / 256 / newZoneMoved  << "M" << std::endl;
    }

    if (useTemp) {
      // make sure GCZone_ is not using by other thread
      auto nReadingPtr = std::move(GCZone_);
      nReadingPtr->lock_.unlock();

      GCZone_ = std::move(tempZone_);
      tempZone_ = std::move(victim);

      GCZone_->lock_.unlock();
      tempZone_->lock_.unlock();

      vectorLock_.lock();
      readingZones_.push_back(nReadingPtr);
      vectorLock_.unlock();
    } else {
      auto nWritingPtr = std::move(victim);
      nWritingPtr->close();
      nWritingPtr->lock_.unlock();

      GCZone_->lock_.unlock();
      tempZone_->lock_.unlock();
      
      vectorLock_.lock();
      writingZones_.push_back(nWritingPtr);
      // writingZones_.insert(writingZones_.begin(), nWritingPtr);
      vectorLock_.unlock();
    }
    
  }
}

/* Mapping Related */
void ZonedDevice::insertMapping(uint64_t znsOffset, uint64_t size, uint64_t logOffset) {
  XLOGF(DBG, "insert logical offset 0x{:X}'s mapping to 0x{:X}, size is 0x{:X}.", logOffset, znsOffset, size);
  auto info = std::make_shared<DataBlockInfo>(znsOffset, size, logOffset);
  mapping_.emplace(logOffset + size, info);
  getZone(znsOffset)->insertLogicalMapping(znsOffset, info);
  // reverseMapping_.emplace(znsOffset, info);
}

void ZonedDevice::deleteMapping(const std::map<uint64_t, std::shared_ptr<DataBlockInfo>>::iterator &mappingIt) {
  XLOGF(DBG, "delete logical offset 0x{:X}'s mapping.", mappingIt->second->logOffset);
  // reverseMapping_.erase(mappingIt->second->znsOffset);
  auto znsOffset = mappingIt->second->znsOffset;
  getZone(znsOffset)->deleteLogicalMapping(znsOffset);
  mapping_.erase(mappingIt);
}

void ZonedDevice::changeMapping(std::shared_ptr<DataBlockInfo> &info, uint64_t newZnsOffset) {
  XLOGF(DBG, "change logical offset 0x{:X}'s mapping from 0x{:X} to 0x{:X}.", info->logOffset, info->znsOffset, newZnsOffset);
  auto znsOffset = info->znsOffset;
  getZone(znsOffset)->deleteLogicalMapping(znsOffset);
  // reverseMapping_.erase(reverseMapping_.find(info->znsOffset));
  info->znsOffset = newZnsOffset;
  getZone(newZnsOffset)->insertLogicalMapping(newZnsOffset, info);
  // reverseMapping_.emplace(newZnsOffset, info);
}

/* Invalid Data Related */
void ZonedDevice::markInvalidSpace(uint64_t offset, uint64_t size) {
  // const std::lock_guard<std::mutex> lock(mappingLock_);
  bool needCheck = false;
  // only insert the end of file map.insert(offset + size, info);
  do {
    auto it = mapping_.lower_bound(offset + 1);
    if (it == mapping_.end()) {
      // there is no value larger than offset
      break;
    }

    const auto [logWrittenEnd, info] = *it;
    auto logWrittenStart = info->logOffset;

    auto overlap = std::max(logWrittenStart, offset) < std::min(logWrittenEnd, offset + size);

    // need do relaim
    if (overlap) {
      // delete current mapping
      // reverseMapping_.erase(it->second->znsOffset);
      // mapping_.erase(it);
      // TODO move
      deleteMapping(it);
      
      auto znsOffset = info->znsOffset;
      // XLOGF(INFO, "mark invalid {} {}", znsOffset, znsOffset + info->size);
      allZones_.at(getZoneId(znsOffset))->markInvalid(znsOffset, znsOffset + info->size);
      // invalidData_.at(getZoneId(znsOffset)).emplace(znsOffset, znsOffset + info->size);
      if (offset + size > logWrittenEnd) {
        needCheck = true;
      }
    }
  } while (needCheck);
}

/* Invalid Data Related */
void ZonedDevice::markInvalidSpaceLocked(uint64_t offset, uint64_t size) {
  // const std::lock_guard<std::mutex> lock(mappingLock_);
  bool needCheck = false;
  // only insert the end of file map.insert(offset + size, info);
  do {
    auto it = mapping_.lower_bound(offset + 1);
    if (it == mapping_.end()) {
      // there is no value larger than offset
      break;
    }

    const auto [logWrittenEnd, info] = *it;
    auto logWrittenStart = info->logOffset;

    auto overlap = std::max(logWrittenStart, offset) < std::min(logWrittenEnd, offset + size);

    // need do relaim
    if (overlap) {
      // delete current mapping
      // reverseMapping_.erase(it->second->znsOffset);
      // mapping_.erase(it);
      // TODO move
      deleteMapping(it);
      
      auto znsOffset = info->znsOffset;
      // XLOGF(INFO, "mark invalid {} {}", znsOffset, znsOffset + info->size);
      allZones_.at(getZoneId(znsOffset))->markInvalidLocked(znsOffset, znsOffset + info->size);
      // invalidData_.at(getZoneId(znsOffset)).emplace(znsOffset, znsOffset + info->size);
      if (offset + size > logWrittenEnd) {
        needCheck = true;
      }
    }
  } while (needCheck);
}


std::unique_ptr<Device> createZonedDevice(
    std::string devPath,
    uint64_t size,
    uint32_t ioAlignSize,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize,
    bool navyZnsRewrite,
    bool navyZnsGCReset) {
      return std::make_unique<ZonedDevice>(devPath, size, ioAlignSize, encryptor, maxDeviceWriteSize, O_RDWR, 
                                           navyZnsRewrite, navyZnsGCReset);
    }

std::unique_ptr<Device> createTestZonedDevice(
    uint64_t size,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t ioAlignSize) {
      XDCHECK(ioAlignSize == 0x1000);
  return std::move(createZonedDevice("/dev/nvme0n2", size, ioAlignSize, encryptor, 1024 * 1024, true, false));
}


 std::unique_ptr<Device> createDirectIoZNSDevice(
     folly::StringPiece file,
     uint64_t size,
     uint32_t ioAlignSize,
     std::shared_ptr<DeviceEncryptor> encryptor,
     uint32_t maxDeviceWriteSize) {
   XDCHECK(folly::isPowTwo(ioAlignSize));
  return std::make_unique<DirectZonedDevice>(file.str(), size, ioAlignSize, encryptor, maxDeviceWriteSize, O_RDWR);
 }

}
}
}