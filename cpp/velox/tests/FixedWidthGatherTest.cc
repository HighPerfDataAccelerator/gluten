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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <random>
#include <vector>

#if defined(__unix__)
#include <sys/mman.h>
#include <unistd.h>
#endif

#include "shuffle/FixedWidthGather.h"

namespace gluten {
namespace {

template <typename T>
class FixedWidthGatherTest : public ::testing::Test {};

using GatherTypes = ::testing::Types<uint8_t, uint16_t, uint32_t, uint64_t, int64_t, __int128_t>;
TYPED_TEST_SUITE(FixedWidthGatherTest, GatherTypes);

TYPED_TEST(FixedWidthGatherTest, preservesBitsAndPartitionBoundaries) {
  using T = TypeParam;
  constexpr uint32_t kRows = 8192;
  constexpr T kSentinel = static_cast<T>(0x5a);
  std::vector<T> src(kRows);
  // Include arbitrary float/double representations and signed decimal values:
  // the gather must preserve the bits rather than convert numeric values.
  std::mt19937 rng(42);
  for (auto& value : src) {
    auto* bytes = reinterpret_cast<uint8_t*>(&value);
    for (size_t byte = 0; byte < sizeof(T); ++byte) {
      bytes[byte] = rng();
    }
  }
  for (uint32_t size : {0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 8192}) {
    SCOPED_TRACE(size);
    // Exact-sized index allocation also checks the SVE index load's width
    // under ASan, including the widening load used by 64-bit gathers.
    std::vector<uint32_t> ids(size);
    for (auto& id : ids) {
      id = rng() % kRows;
    }
    if (size > 1) {
      ids.front() = kRows - 1;
      ids.back() = 0;
    }
    for (uint32_t offset : {1, 2, 3, 4, 5}) {
      SCOPED_TRACE(offset);
      for (bool packed : {false, true}) {
        std::vector<T> dst(size + offset + 1, kSentinel);
        detail::gatherFixedWidth(src.data(), ids.data(), size, dst.data() + offset, true, packed);
        for (uint32_t row = 0; row < size; ++row) {
          EXPECT_EQ(std::memcmp(&src[ids[row]], &dst[offset + row], sizeof(T)), 0);
        }
        for (uint32_t row = 0; row < offset; ++row) {
          EXPECT_TRUE(dst[row] == kSentinel);
        }
        EXPECT_TRUE(dst.back() == kSentinel);
      }
    }
  }
}

TYPED_TEST(FixedWidthGatherTest, repeatedAndReverseSelections) {
  using T = TypeParam;
  std::array<T, 257> src;
  for (uint32_t row = 0; row < src.size(); ++row) {
    src[row] = static_cast<T>(row);
  }
  std::array<uint32_t, 257> ids;
  std::array<T, 257> dst;
  for (bool repeated : {false, true}) {
    for (uint32_t row = 0; row < ids.size(); ++row) {
      ids[row] = repeated ? 17 : ids.size() - row - 1;
    }
    detail::gatherFixedWidth(src.data(), ids.data(), ids.size(), dst.data());
    for (uint32_t row = 0; row < ids.size(); ++row) {
      EXPECT_TRUE(dst[row] == src[ids[row]]);
    }
  }
}

TYPED_TEST(FixedWidthGatherTest, runtimeDisableMatchesSvePath) {
  using T = TypeParam;
  std::array<T, 259> input;
  std::array<uint32_t, 259> indices;
  std::array<T, 259> vectorized;
  std::array<T, 259> scalar;
  for (uint32_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<T>(i * 17);
    indices[i] = (i * 37) % input.size();
  }
  detail::gatherFixedWidth(input.data(), indices.data(), indices.size(), vectorized.data(), true);
  detail::gatherFixedWidth(input.data(), indices.data(), indices.size(), scalar.data(), false);
  EXPECT_EQ(std::memcmp(vectorized.data(), scalar.data(), sizeof(vectorized)), 0);
}

#if defined(__unix__)
// SVE memory accesses need a real guard page as well as sanitizer coverage.
// In particular, an ordinary 32-bit vector load for 64-bit gather indices
// would incorrectly read twice as many IDs as it consumes.
class GuardedPage {
 public:
  GuardedPage() : size_(sysconf(_SC_PAGESIZE)) {
    data_ = mmap(nullptr, size_ * 2, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (data_ != MAP_FAILED) {
      protected_ = mprotect(static_cast<char*>(data_) + size_, size_, PROT_NONE) == 0;
    }
  }

  ~GuardedPage() {
    if (data_ != MAP_FAILED) {
      munmap(data_, size_ * 2);
    }
  }

  bool valid() const {
    return protected_;
  }

  template <typename T>
  T* tail(uint32_t size) {
    return reinterpret_cast<T*>(static_cast<char*>(data_) + size_) - size;
  }

 private:
  const size_t size_;
  void* data_;
  bool protected_{false};
};

TYPED_TEST(FixedWidthGatherTest, doesNotAccessPastBuffers) {
  using T = TypeParam;
  GuardedPage source;
  GuardedPage indices;
  GuardedPage destination;
  ASSERT_TRUE(source.valid());
  ASSERT_TRUE(indices.valid());
  ASSERT_TRUE(destination.valid());
  auto* src = source.tail<T>(128);
  for (uint32_t row = 0; row < 128; ++row) {
    src[row] = static_cast<T>(row);
  }
  for (uint32_t size = 0; size <= 65; ++size) {
    SCOPED_TRACE(size);
    auto* ids = indices.tail<uint32_t>(size);
    auto* dst = destination.tail<T>(size);
    for (uint32_t row = 0; row < size; ++row) {
      ids[row] = 127 - row;
    }
    for (bool packed : {false, true}) {
      std::fill_n(dst, size, T{});
      detail::gatherFixedWidth(src, ids, size, dst, true, packed);
      for (uint32_t row = 0; row < size; ++row) {
        EXPECT_TRUE(dst[row] == src[ids[row]]);
      }
    }
  }
}
#endif

} // namespace
} // namespace gluten
