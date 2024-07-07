#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <pthread.h>
#include <atomic>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <algorithm>

typedef struct {
    JobState job_state;
    const InputVec& inputVec; // TODO maybe change &
    OutputVec& outputVec;
    std::atomic<int>* atomic_counter;
    pthread_t main_thread;
    const MapReduceClient& client;
    Barrier* barrier;
} ClientContext;

typedef struct {
    ClientContext* client_context;
    IntermediateVec& intermediate_vec;
} ThreadContext;

bool compareIntermediatePair(const IntermediatePair& pair1, const IntermediatePair& pair2) {
  return pair1.first < pair2.first;
}

void* thread_entry_point(void *arg) {
  ClientContext* client_context = (ClientContext*) arg;

  // define thread_context
  ThreadContext* thread_context = new ThreadContext {
      client_context,
      *(new IntermediateVec())
  };

  // map phase
  const InputPair* curr_pair;
  int prev_count;
  while ((prev_count = (client_context->atomic_counter->fetch_add(1))) <
  client_context->inputVec.size()) {
    curr_pair = &(client_context->inputVec[prev_count]);
    client_context->client.map(curr_pair->first, curr_pair->second,
                           thread_context);
  }

  // sort phase
  std::sort(thread_context->intermediate_vec.begin(),
            thread_context->intermediate_vec.end(),
            compareIntermediatePair);

  // barrier
  client_context->barrier->barrier();

  // reduce phase
  // TODO use semaphore to wait until shuffle finished

  pthread_exit(NULL);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
  Barrier barrier(multiThreadLevel);

  // define client_context
  ClientContext* client_context = new ClientContext {
      {UNDEFINED_STAGE, 0},
      inputVec,
      outputVec,
      new std::atomic<int>(0),
      pthread_self(),
      client,
      &barrier
  };

  // create threads
  pthread_t threads[multiThreadLevel];
  int ret;
  for(int i = 0; i < multiThreadLevel; i++) {
    printf("Creating thread %d\n", i);
    ret = pthread_create(&threads[i], NULL, thread_entry_point,client_context);
    if (ret) {
      printf("ERROR; return code from pthread_create() is %d\n", ret);
      exit(-1);
    }
  }
  for (int i = 0; i < multiThreadLevel; ++i) {
    pthread_join(threads[i], NULL);
  }
  int i = *(client_context->atomic_counter);
  printf("%d\n", i);
  printf("bye\n");
  return (JobHandle) client_context;
}