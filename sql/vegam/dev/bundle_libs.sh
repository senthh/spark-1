#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Pack libvegam.so and every shared library it needs that is not part of the
# base OS into one tarball, for spark-submit --archives vegamlib.tgz#vegamlib
# with LD_LIBRARY_PATH=./vegamlib on the executors.
#
# Usage: bundle_libs.sh <libvegam.so> <out.tgz> [extra lib dir ...]
# Extra dirs are searched for libraries ldd cannot resolve (e.g. Velox's icu).

set -euo pipefail

LIB="$1"
OUT="$2"
shift 2
EXTRA=("$@")

STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT

# Libraries every node already has; shipping them would shadow the OS copy.
BASE='^(linux-vdso|ld-linux|libc|libm|libdl|librt|libpthread|libresolv|libutil'
BASE+='|libgcc_s|libstdc\+\+|libz|libcrypt|libselinux|libjvm)\.'

export LD_LIBRARY_PATH="$(IFS=:; echo "${EXTRA[*]:-}")${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

cp -L "${LIB}" "${STAGE}/"
ldd "${LIB}" | while read -r name arrow path _; do
  base="$(basename "${name}")"
  if [[ "${base}" =~ ${BASE} ]]; then
    continue
  fi
  if [ "${arrow}" = "=>" ] && [ -f "${path}" ]; then
    cp -L "${path}" "${STAGE}/${base}"
  elif [ "${arrow}" = "=>" ]; then
    echo "unresolved: ${base}" >&2
    exit 1
  fi
done

chmod u+w "${STAGE}"/*
strip --strip-debug "${STAGE}"/*

MISSING="$(cd "${STAGE}" && LD_LIBRARY_PATH="${STAGE}" ldd ./*.so* | grep "not found" || true)"
if [ -n "${MISSING}" ]; then
  echo "bundle is not self-contained:" >&2
  echo "${MISSING}" >&2
  exit 1
fi
tar -czf "${OUT}" -C "${STAGE}" .
echo "wrote ${OUT}: $(ls "${STAGE}" | wc -l) libs, $(du -sh "${OUT}" | cut -f1)"
