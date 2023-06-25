#!/usr/bin/env bash

current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )";
cd ${current_dir}

release_name=`date +%Y%m%d%H%M%S`
src_dir=${current_dir}/target/release
target_dir=${current_dir}/releases/${release_name}
link_file=${current_dir}/releases/current 
mkdir -p ${target_dir}
[ -d "${src_dir}" ] && cp -r ${src_dir}/* ${target_dir}/ \
 && rm -f ${link_file} && ln -fs ${target_dir} ${link_file}