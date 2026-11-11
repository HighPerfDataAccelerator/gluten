#!/usr/bin/env bash
# usage: cfg.sh <gen|use|plain>  — configures+builds Velox in /data/nvme1/pgo/velox-<mode>
set -euo pipefail
mode=$1
source /home/nvidia/ferd/benchmarks/perf-optimization-20261029/env.sh
export VELOX_HOME=/data/nvme1/pgo/velox-src
export CPU_TARGET=aarch64 ARM_BUILD_TARGET=local
export VELOX_DEPENDENCY_SOURCE=SYSTEM simdjson_SOURCE=SYSTEM Arrow_SOURCE=SYSTEM
export PKG_CONFIG_LIBDIR="$VCPKG_MANIFEST_DIR/vcpkg_installed/$VCPKG_TRIPLET/lib/pkgconfig:$VCPKG_MANIFEST_DIR/vcpkg_installed/$VCPKG_TRIPLET/share/pkgconfig"
export PATH="$VCPKG_MANIFEST_DIR/vcpkg_installed/$VCPKG_TRIPLET/tools/protobuf:$PATH"
B=/data/nvme1/pgo/velox-$mode
G=/data/nvme1/pgo/gcda
base='-O3 -DNDEBUG -mcpu=olympus -fno-stack-protector -fno-stack-clash-protection -U_FORTIFY_SOURCE -fzero-init-padding-bits=standard'
case $mode in
 gen) extra="-fprofile-generate=$G -fprofile-update=prefer-atomic -fprofile-prefix-path=$B" ;;
 use) extra="-fprofile-use=$G -fprofile-correction -Wno-missing-profile -Wno-error=coverage-mismatch -fprofile-prefix-path=$B" ;;
 usel) extra="-fprofile-use=$G -fprofile-correction -Wno-missing-profile -Wno-error=coverage-mismatch -fprofile-prefix-path=$B -flto=auto" ;;
esac
flags="$base $extra"
mkdir -p $B
cmake -S $VELOX_HOME -B $B -G Ninja -C /data/nvme1/pgo/scripts/velox-initial.cmake \
  -DCMAKE_TOOLCHAIN_FILE=/home/nvidia/ferd/code/gluten/dev/vcpkg/toolchain.cmake \
  -DCMAKE_MAKE_PROGRAM=/home/nvidia/ferd/.deps/root/usr/bin/ninja \
  -DCMAKE_FIND_PACKAGE_PREFER_CONFIG=OFF -DFLEX_INCLUDE_DIR=/home/nvidia/ferd/.deps/flex/usr/include -DCMAKE_EXPORT_COMPILE_COMMANDS=ON "-DCMAKE_CXX_FLAGS_RELEASE=$flags" "-DCMAKE_C_FLAGS_RELEASE=$flags"
