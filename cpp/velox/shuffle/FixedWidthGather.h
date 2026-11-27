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

#pragma once

#include <cstdint>
#include <type_traits>

#if defined(__ARM_FEATURE_SVE)
#include <arm_sve.h>
#endif

#if defined(__linux__) && defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && defined(__AARCH64EL__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif

namespace gluten::detail {

inline bool supportsPackedGather() {
#if defined(__linux__) && defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && defined(__AARCH64EL__)
  static const bool isVera = [] {
    if ((getauxval(AT_HWCAP) & (HWCAP_CPUID | HWCAP_SVE)) != (HWCAP_CPUID | HWCAP_SVE)) {
      return false;
    }
    uint64_t midr;
    asm("mrs %0, midr_el1" : "=r"(midr));
    return (midr & 0xff00fff0) == 0x4e000100;
  }();
  return isVera && svcntb() == 16;
#else
  return false;
#endif
}

#if defined(__ARM_FEATURE_SVE)
// Each 128-bit vector holds two gathered values. Describe the full 32-byte
// write to the compiler so adjacent partitions and unaligned appends are safe.
inline void storeGatherPair(uint64_t* dst, svuint64_t first, svuint64_t second) {
  asm("stp %q2, %q3, [%1]" : "=m"(*reinterpret_cast<unsigned char(*)[32]>(dst)) : "r"(dst), "w"(first), "w"(second));
}
#endif

// Copy one partition's row selection to its contiguous value buffer. Source,
// rowIds and destination must not overlap. Small selections and other widths
// retain the scalar path. No vector load reads past the selection's end.
template <typename T>
inline void gatherFixedWidth(
    const T* __restrict__ src,
    const uint32_t* __restrict__ rowIds,
    uint32_t size,
    T* __restrict__ dst,
    bool enableSveGather = true,
    [[maybe_unused]] bool packIndices = false) {
  uint32_t pos = 0;
#if defined(__ARM_FEATURE_SVE)
  if constexpr (std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t> || std::is_same_v<T, int64_t>) {
    // Avoid vector setup for the tiny partitions common with many reducers.
    constexpr uint32_t kMinVectorizedRows = 8;
    if (!enableSveGather || size < kMinVectorizedRows) {
      for (; pos < size; ++pos) {
        dst[pos] = src[rowIds[pos]];
      }
      return;
    }
    // Vera has 128-bit SVE vectors. Use fixed loop strides on this target
    // and overlap the two independent gathers in each iteration.
    if (svcntb() == 16) {
      const auto* indices = rowIds;
      auto* output = dst;
      if constexpr (std::is_same_v<T, uint32_t>) {
        const auto all = svptrue_b32();
        const auto* blockEnd = rowIds + (size & ~uint32_t{7});
        for (; indices < blockEnd; indices += 8, output += 8) {
          const auto indices0 = svld1_u32(all, indices);
          const auto indices1 = svld1_u32(all, indices + 4);
          const auto values0 = svld1_gather_u32index_u32(all, src, indices0);
          const auto values1 = svld1_gather_u32index_u32(all, src, indices1);
          svst1_u32(all, output, values0);
          svst1_u32(all, output + 4, values1);
        }
      } else {
        const auto all = svptrue_b64();
        const auto* data = reinterpret_cast<const uint64_t*>(src);
        if (packIndices) {
          const auto* packedEnd = rowIds + (size & ~uint32_t{7});
          for (; indices < packedEnd; indices += 8, output += 8) {
            const auto first = svld1_u32(svptrue_b32(), indices);
            const auto second = svld1_u32(svptrue_b32(), indices + 4);
            const auto a = svld1_gather_u64index_u64(all, data, svunpklo_u64(first));
            const auto b = svld1_gather_u64index_u64(all, data, svunpkhi_u64(first));
            const auto c = svld1_gather_u64index_u64(all, data, svunpklo_u64(second));
            const auto d = svld1_gather_u64index_u64(all, data, svunpkhi_u64(second));
            storeGatherPair(reinterpret_cast<uint64_t*>(output), a, b);
            storeGatherPair(reinterpret_cast<uint64_t*>(output + 4), c, d);
          }
        }
        const auto* blockEnd = rowIds + (size & ~uint32_t{3});
        for (; indices < blockEnd; indices += 4, output += 4) {
          const auto indices0 = svld1uw_u64(all, indices);
          const auto indices1 = svld1uw_u64(all, indices + 2);
          const auto values0 = svld1_gather_u64index_u64(all, data, indices0);
          const auto values1 = svld1_gather_u64index_u64(all, data, indices1);
          svst1_u64(all, reinterpret_cast<uint64_t*>(output), values0);
          svst1_u64(all, reinterpret_cast<uint64_t*>(output + 2), values1);
        }
      }
      const auto* end = rowIds + size;
      for (; indices < end; ++indices, ++output) {
        *output = src[*indices];
      }
      return;
    }
    if constexpr (std::is_same_v<T, uint32_t>) {
      const auto lanes = svcntw();
      const auto all = svptrue_b32();
      for (; size - pos >= lanes; pos += lanes) {
        auto indices = svld1_u32(all, rowIds + pos);
        svst1_u32(all, dst + pos, svld1_gather_u32index_u32(all, src, indices));
      }
    } else {
      const auto lanes = svcntd();
      const auto all = svptrue_b64();
      for (; size - pos >= lanes; pos += lanes) {
        // Zero-extend the 32-bit row IDs, loading only one per 64-bit lane.
        auto indices = svld1uw_u64(all, rowIds + pos);
        auto values = svld1_gather_u64index_u64(all, reinterpret_cast<const uint64_t*>(src), indices);
        svst1_u64(all, reinterpret_cast<uint64_t*>(dst + pos), values);
      }
    }
  }
#endif
  for (; pos < size; ++pos) {
    dst[pos] = src[rowIds[pos]];
  }
}

} // namespace gluten::detail
