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
// Build on Linux with: cc -O2 -Wall -pthread shuffle-relay.c -lrdmacm -o gluten-rdma-relay
// One listener per peer. Spark owns framing, authentication, encryption and retry policy.
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <rdma/rsocket.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static bool ingress;
static struct sockaddr_in destination;

static struct sockaddr_in address(const char* host, const char* port) {
  struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM, .ai_flags = AI_NUMERICSERV};
  struct addrinfo* result;
  int error = getaddrinfo(host, port, &hints, &result);
  if (error) {
    fprintf(stderr, "Cannot resolve %s:%s: %s\n", host, port, gai_strerror(error));
    exit(1);
  }
  struct sockaddr_in addr = *(struct sockaddr_in*)result->ai_addr;
  freeaddrinfo(result);
  return addr;
}

struct stream {
  char data[65536];
  size_t offset;
  size_t end;
  bool eof;
};

static bool temporary(void) {
  return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
}

// Both directions have independent bounded buffers. rpoll accepts ordinary and RDMA descriptors.
static void transfer(int tcp, int rdma) {
  struct pollfd fd[2] = {{.fd = tcp}, {.fd = rdma}};
  struct stream buffers[2] = {0};
  if (fcntl(tcp, F_SETFL, O_NONBLOCK) || rfcntl(rdma, F_SETFL, O_NONBLOCK)) {
    return;
  }
  while (!(buffers[0].eof && buffers[1].eof)) {
    for (int i = 0; i < 2; i++) {
      fd[i].events = (!buffers[i].eof && buffers[i].end == 0 ? POLLIN : 0) | (buffers[1 - i].end ? POLLOUT : 0);
    }
    if (rpoll(fd, 2, -1) < 0) {
      if (errno == EINTR)
        continue;
      return;
    }
    for (int i = 0; i < 2; i++) {
      if (fd[i].revents & (POLLERR | POLLNVAL))
        return;
      struct stream* out = &buffers[1 - i];
      if (fd[i].revents & POLLOUT && out->end) {
        size_t len = out->end - out->offset;
        ssize_t sent = i ? rsend(rdma, out->data + out->offset, len, MSG_NOSIGNAL)
                         : send(tcp, out->data + out->offset, len, MSG_NOSIGNAL);
        if (sent < 0 && !temporary())
          return;
        if (sent > 0) {
          out->offset += sent;
          if (out->offset == out->end)
            out->offset = out->end = 0;
        }
      }
      struct stream* in = &buffers[i];
      if (fd[i].revents & (POLLIN | POLLHUP) && !in->eof && !in->end) {
        ssize_t got = i ? rrecv(rdma, in->data, sizeof(in->data), 0) : recv(tcp, in->data, sizeof(in->data), 0);
        if (got > 0)
          in->end = got;
        if (got < 0 && !temporary())
          return;
        if (got == 0) {
          in->eof = true;
          if (i)
            shutdown(tcp, SHUT_WR);
          else
            rshutdown(rdma, SHUT_WR);
        }
      }
    }
  }
}

static void* connection(void* arg) {
  int peer = *(int*)arg;
  free(arg);
  int tcp = ingress ? socket(AF_INET, SOCK_STREAM, 0) : peer;
  int rdma = ingress ? peer : rsocket(AF_INET, SOCK_STREAM, 0);
  if (tcp >= 0 && rdma >= 0) {
    struct sockaddr* dest = (struct sockaddr*)&destination;
    int rc = ingress ? connect(tcp, dest, sizeof(destination)) : rconnect(rdma, dest, sizeof(destination));
    if (!rc)
      transfer(tcp, rdma);
    else
      perror("shuffle relay connect");
  }
  if (tcp >= 0)
    close(tcp);
  if (rdma >= 0)
    rclose(rdma);
  return NULL;
}

// The JVM owns stdin. Also terminate on executor crashes that bypass ShuffleManager.stop().
static void* watchParent(void* unused) {
  (void)unused;
  char ch;
  ssize_t n;
  do {
    n = read(STDIN_FILENO, &ch, 1);
  } while (n > 0 || (n < 0 && errno == EINTR));
  _exit(0);
}

int main(int argc, char** argv) {
  if (argc != 6 || (strcmp(argv[1], "ingress") && strcmp(argv[1], "egress")))
    return 2;
  ingress = !strcmp(argv[1], "ingress");
  struct sockaddr_in local = address(argv[2], argv[3]);
  destination = address(argv[4], argv[5]);
  signal(SIGPIPE, SIG_IGN);
  pthread_t watcher;
  if (pthread_create(&watcher, NULL, watchParent, NULL))
    return 1;
  alarm(15); // Bound startup while the shuffle manager waits for the listening port.
  int sock = ingress ? rsocket(AF_INET, SOCK_STREAM, 0) : socket(AF_INET, SOCK_STREAM, 0);
  if (!ingress && sock >= 0) {
    // Rebind the same loopback port after a relay exits while connections are in TIME_WAIT.
    int enabled = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled))) {
      perror("shuffle relay SO_REUSEADDR");
      return 1;
    }
  }
  struct sockaddr* addr = (struct sockaddr*)&local;
  socklen_t len = sizeof(local);
  if (sock < 0 || (ingress ? rbind(sock, addr, len) : bind(sock, addr, len)) ||
      (ingress ? rlisten(sock, 128) : listen(sock, 128)) ||
      (ingress ? rgetsockname(sock, addr, &len) : getsockname(sock, addr, &len))) {
    perror("shuffle relay listen");
    return 1;
  }
  alarm(0);
  printf("%d\n", ntohs(local.sin_port));
  fflush(stdout);
  for (;;) {
    int peer = ingress ? raccept(sock, NULL, NULL) : accept(sock, NULL, NULL);
    if (peer < 0) {
      if (errno == EINTR || errno == ECONNABORTED)
        continue;
      perror("shuffle relay accept");
      return 1;
    }
    int* arg = malloc(sizeof(int));
    pthread_t worker;
    if (arg) {
      *arg = peer;
      if (!pthread_create(&worker, NULL, connection, arg)) {
        pthread_detach(worker);
        continue;
      }
      free(arg);
    }
    if (ingress)
      rclose(peer);
    else
      close(peer);
  }
}
