#!/bin/bash
#如果你想要提高效率并利用并发，可以使用 & 在 Bash 中运行命令作为后台进程，然后使用 wait 命令等待所有后台进程完成。以下是一个修改过的脚本，使用10个并发进程发送数据：
ENDPOINT="http://localhost:8428/api/v1/import/prometheus"
TOTAL_REQUESTS=100000
CONCURRENCY=10
REQUESTS_PER_CONCURRENCY=$((TOTAL_REQUESTS / CONCURRENCY))

send_requests() {
  for i in $(seq 1 $REQUESTS_PER_CONCURRENCY); do
    # 生成随机标签值
    RAND_LABEL_VALUE=$(cat /dev/urandom | env LC_CTYPE=C tr -dc A-Za-z0-9 | head -c 13)
    # 构建数据
    DATA="my_metric{label=\"$RAND_LABEL_VALUE\"} $i"
    # 使用curl发送数据
    curl -d "$DATA" $ENDPOINT -s > /dev/null
  done
}

# 启动并发进程
for i in $(seq 1 $CONCURRENCY); do
  send_requests &
done

# 等待所有后台进程完成
wait

echo "Finished sending data."