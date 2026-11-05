/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/concurrency/ConcurrentHashMap.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

namespace gluten {
namespace {

#if FOLLY_F14_CRC_INTRINSIC_AVAILABLE
struct CollidingHash {
  size_t operator()(uint64_t) const {
    return 0xdeadbeef;
  }
};

template <typename Hash>
void checkSimdMap() {
  constexpr uint64_t kCount = 2048;
  folly::ConcurrentHashMapSIMD<uint64_t, uint64_t, Hash> map;
  std::vector<std::thread> writers;
  for (uint64_t lane = 0; lane < 4; ++lane) {
    writers.emplace_back([&, lane] {
      for (auto i = lane; i < kCount; i += 4) {
        map.insert_or_assign(i, i * 7);
      }
    });
  }
  for (auto& writer : writers) {
    writer.join();
  }
  ASSERT_EQ(map.size(), kCount);
  for (uint64_t i = 0; i < kCount; ++i) {
    const auto it = map.find(i);
    ASSERT_NE(it, map.end());
    ASSERT_EQ(it->second, i * 7);
  }
  for (uint64_t i = 0; i < kCount; i += 3) {
    map.insert_or_assign(i, i * 7 + 1);
  }
  for (uint64_t i = 0; i < kCount; i += 2) {
    ASSERT_EQ(map.erase(i), 1);
  }
  ASSERT_EQ(map.size(), kCount / 2);
  for (uint64_t i = 0; i < kCount * 2; ++i) {
    const auto it = map.find(i);
    if (i >= kCount || i % 2 == 0) {
      ASSERT_EQ(it, map.end());
    } else {
      ASSERT_NE(it, map.end());
      ASSERT_EQ(it->second, i * 7 + (i % 3 == 0));
    }
  }
}
#endif

TEST(FollyConcurrentHashMapTest, SimdLookupWithConcurrentWritersAndCollisions) {
#if FOLLY_F14_CRC_INTRINSIC_AVAILABLE
  // Compiling this template with SVE2 exercises the NEON/SVE byte bridge in
  // Folly's tag matching, including duplicate tags from colliding hashes.
  checkSimdMap<std::hash<uint64_t>>();
  checkSimdMap<CollidingHash>();
#else
  GTEST_SKIP() << "Folly SIMD hash map is unavailable on this target";
#endif
}

} // namespace
} // namespace gluten
