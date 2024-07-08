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
#include <semaphore.h>


typedef struct {
    // general
    const MapReduceClient& client;
    JobState job_state;
    pthread_t main_thread;
    // vectors
    const InputVec& inputVec;
    std::vector<IntermediateVec> intermediate_vecs;
    OutputVec& outputVec;
    // locks
    std::atomic<int>* atomic_counter;
    Barrier* barrier;
    sem_t shuffle_semaphore;
    pthread_mutex_t vector_mutex;
} ClientContext;

typedef struct {
    int thread_id;
    ClientContext* client_context;
    IntermediateVec& intermediate_vec;
} ThreadContext;

bool compareIntermediatePair(const IntermediatePair& pair1, const IntermediatePair& pair2) {
  return pair1.first < pair2.first;
}

void* thread_entry_point(void *arg) {
  ThreadContext* thread_context = (ThreadContext*) arg;
  ClientContext* client_context = thread_context->client_context;

  // map phase
  const InputPair* curr_pair;
  int prev_count;
  while ((prev_count = (client_context->atomic_counter->fetch_add(1))) <
  client_context->inputVec.size()) {
    std::cout << "Thread " << thread_context->thread_id << " map phase\n";
    curr_pair = &(client_context->inputVec[prev_count]);
    client_context->client.map(curr_pair->first, curr_pair->second,
                           thread_context);
  }

  // sort phase
  std::cout << "Thread " << thread_context->thread_id << " sort phase\n";
  std::sort(thread_context->intermediate_vec.begin(),
            thread_context->intermediate_vec.end(),
            compareIntermediatePair);

  // barrier
  client_context->barrier->barrier();

  // shuffle phase
  if (thread_context->thread_id == 0) {
    std::cout << "Thread " << thread_context->thread_id << " shuffle phase\n";
    sem_post(&client_context->shuffle_semaphore);
    client_context->atomic_counter->store(0);
  }
  else {
    sem_wait(&client_context->shuffle_semaphore);
    sem_post(&client_context->shuffle_semaphore);
  }
  std::cout << "Thread " << thread_context->thread_id << " reducing phase\n";

  // reduce phase


  pthread_exit(NULL);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
  Barrier barrier(multiThreadLevel);

  // define client_context
  ClientContext* client_context = new ClientContext {
    client,
    {UNDEFINED_STAGE, 0},
    pthread_self(),
    inputVec,
    *(new std::vector<IntermediateVec>(multiThreadLevel)),
    outputVec,
    new std::atomic<int>(0),
    &barrier,
    sem_t(),
    PTHREAD_MUTEX_INITIALIZER
  };
  sem_init(&(client_context->shuffle_semaphore), 0, 0);

  // create threads
  pthread_t threads[multiThreadLevel];
  int ret;
  ThreadContext* thread_context;
  for(int i = 0; i < multiThreadLevel; i++) {
    printf("Creating thread %d\n", i);
    thread_context = new ThreadContext {
      i,
      client_context,
      client_context->intermediate_vecs[i]
    };
    ret = pthread_create(&threads[i], NULL, thread_entry_point, thread_context);
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