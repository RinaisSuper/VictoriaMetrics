#!/bin/bash

ENDPOINT="http://localhost:8428/api/v1/import/prometheus"

for i in $(seq 1 100000); do
  # 生成随机标签值
  RAND_LABEL_VALUE=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 13 ; echo '')
  # 构建数据
  DATA="my_metric{label=\"$RAND_LABEL_VALUE\"} $i"
  # 使用curl发送数据
  curl -d "$DATA" $ENDPOINT
  # 暂停一下以避免过快的请求（可选）
  sleep 0.01
done

echo "Finished sending data."
