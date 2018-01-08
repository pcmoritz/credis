#include <chrono>
#include <vector>

#include "glog/logging.h"

#include "client.h"

// To launch with 2 servers:
//
//   pkill -f redis-server; ./setup.sh 2; make -j;
//   ./src/credis_seqput_bench 2
//
// If "2" is omitted in the above, by default 1 server is used.

const int N = 500000;
int num_completed = 0;
aeEventLoop* loop = aeCreateEventLoop(64);
redisAsyncContext* write_context = nullptr;
int init_subscribe_ack = 0;

void SeqPutAckCallback(redisAsyncContext* ack_context,  // != write_context.
                       void* r,
                       void* privdata) {
  // CHECK(write_context != ack_context) << write_context << " " << ack_context;
  /* Replies to the SUBSCRIBE command have 3 elements. There are two
   * possibilities. Either the reply is the initial acknowledgment of the
   * subscribe command, or it is a message. If it is the initial acknowledgment,
   * then
   *     - reply->element[0]->str is "subscribe"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is null.
   * If it is an actual message, then
   *     - reply->element[0]->str is "message"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is the contents of the message.
   */
  // if (r == nullptr) {
  //   LOG(INFO) << "null reply received";
  //   return;
  // }
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  // CHECK(reply->type == REDIS_REPLY_ARRAY);
  // CHECK(reply->elements == 3);
  const redisReply* message_type = reply->element[0];
  // LOG(INFO) << message_type->str;

  // NOTE(zongheng): this is a hack.
  // if (strcmp(message_type->str, "message") == 0) {
  if (!init_subscribe_ack) {
    init_subscribe_ack = 1;
    return;
  }

  // const int seqnum = std::stoi(reply->element[2]->str);
  // latencies[seqnum] =
  //     std::chrono::duration<float, std::micro>(now - time_starts[seqnum])
  //         .count();
  ++num_completed;
  if (num_completed == N) {
    aeStop(loop);
    return;
  }

  // Launch next pair.
  // LOG(INFO) << "launching i = " << num_completed;
  const std::string s = std::to_string(num_completed);
  const int status = redisAsyncCommand(write_context, /*callback=*/NULL,
                                       /*privdata=*/NULL, "MEMBER.PUT %b %b",
                                       s.data(), s.size(), s.data(), s.size());
  CHECK(status == REDIS_OK);
}

int main(int argc, char** argv) {
  // Parse.
  int num_chain_nodes = 1;
  if (argc > 1) num_chain_nodes = std::stoi(argv[1]);
  // Set up "write_port" and "ack_port".
  const int write_port = 6370;
  const int ack_port = write_port + num_chain_nodes - 1;
  LOG(INFO) << "num_chain_nodes " << num_chain_nodes << " write_port "
            << write_port << " ack_port " << ack_port;

  RedisClient client;
  CHECK(client.Connect("127.0.0.1", write_port, ack_port).ok());
  CHECK(client.AttachToEventLoop(loop).ok());
  CHECK(client
            .RegisterAckCallback(
                static_cast<redisCallbackFn*>(&SeqPutAckCallback))
            .ok());
  write_context = client.async_context();

  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // SeqPut.  Start with "0->0", and each callback will launch the next pair.
  const std::string kZeroStr = "0";
  const int status =
      redisAsyncCommand(write_context, /*callback=*/NULL,
                        /*privdata=*/NULL, "MEMBER.PUT %b %b", kZeroStr.data(),
                        kZeroStr.size(), kZeroStr.data(), kZeroStr.size());
  CHECK(status == REDIS_OK);

  LOG(INFO) << "start loop";
  aeMain(loop);
  LOG(INFO) << "end loop";

  auto end = std::chrono::system_clock::now();
  CHECK(num_completed == N)
      << "num_completed " << num_completed << " vs N " << N;
  LOG(INFO) << "ending bench";

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, total duration (ms) " << latency_us / 1e3
            << ", num_ops " << N << ", num_nodes " << num_chain_nodes;

  return 0;
}
