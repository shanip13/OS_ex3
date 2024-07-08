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
    JobState* job_state;
    pthread_t main_thread;
    // vectors
    const InputVec& inputVec;
    std::vector<IntermediateVec> intermediate_vecs;
    std::vector<IntermediateVec> shuffled_queue;
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

void waitForJob(JobHandle job) {
  auto* context = static_cast<ClientContext*>(job);
  if (pthread_join(context->main_thread, nullptr) != 0) {
      std::cerr << "Error joining job thread.\n";
  }
}

void getJobState(JobHandle job, JobState* state){
  auto* context = static_cast<ClientContext*>(job);
  *state = *(context->job_state);
}

void closeJobHandle(JobHandle job){
    if (job == nullptr) {
        return;
    }
    auto* context = static_cast<ClientContext*>(job);

    pthread_join(context->main_thread, nullptr);
    if (sem_destroy(&context->shuffle_semaphore) != 0) {
        std::cerr << "Failed to destroy semaphore" << std::endl;  // TODO maybe delete
    }
    delete context->atomic_counter;
    delete context;
}



bool compareIntermediatePair(const IntermediatePair& pair1, const IntermediatePair& pair2) {
  return pair1.first < pair2.first;
}

bool K2_equals(K2* key1, K2* key2) {
  return (not (*key1 < *key2)) && (not (*key2 < *key1));
}

int shuffle(ClientContext* client_context) {
  std::vector<IntermediateVec> &intermediate_vecs = client_context->intermediate_vecs;
  std::vector<IntermediateVec> &shuffled_queue = client_context->shuffled_queue;
  // get num of intermediate elements
  size_t intermediate_size = 0;
  for (const auto& vec : intermediate_vecs) {
    intermediate_size += vec.size();
  }

  while (not intermediate_vecs.empty()) {
    // delete empty vectors
    auto it = intermediate_vecs.begin();
    while (it != intermediate_vecs.end()) {
      if (it->empty()) { it = intermediate_vecs.erase(it);}
      else {++it;}
    }
    if (intermediate_vecs.empty()) {break;}

    // find the largest key
    K2* curr_key;
    K2* largest_key = intermediate_vecs[0][0].first;
    for (auto& vec : intermediate_vecs) {
      curr_key = vec.back().first;
      if (*largest_key < *curr_key) {
        largest_key = curr_key;
      }
    }

    // pop elements with the largest key into new sequence
    shuffled_queue.push_back(*(new IntermediateVec()));
    for (auto& vec : intermediate_vecs) {
      while (!vec.empty() && K2_equals(vec.back().first, largest_key)) {
        shuffled_queue.back().push_back(vec.back());
        vec.pop_back();
        client_context->job_state->percentage += 100.0/intermediate_size;
      }
    }
  }
  return 0;
}

void* thread_entry_point(void *arg) {
  ThreadContext* thread_context = (ThreadContext*) arg;
  ClientContext* client_context = thread_context->client_context;

  // map phase
  client_context->job_state->stage = MAP_STAGE;
  const InputPair* curr_pair;
  int prev_count;
  size_t input_size = client_context->inputVec.size();
  while ((prev_count = (client_context->atomic_counter->fetch_add(1))) < input_size) {
    client_context->job_state->percentage =
        100.0*client_context->atomic_counter->load()/input_size;
    curr_pair = &(client_context->inputVec[prev_count]);
    client_context->client.map(curr_pair->first, curr_pair->second,
                           thread_context);
    // TODO map uses emit2 to put results into thread_context->intermediatevec
  }

  // sort phase
  std::sort(thread_context->intermediate_vec.begin(),
            thread_context->intermediate_vec.end(),
            compareIntermediatePair);

  // barrier
  client_context->barrier->barrier();

  // shuffle phase
  if (thread_context->thread_id == 0) {
    client_context->job_state->stage = SHUFFLE_STAGE;
    client_context->job_state->percentage=0;
    shuffle(client_context);
    sem_post(&client_context->shuffle_semaphore);
    client_context->atomic_counter->store(0);
    client_context->job_state->stage = REDUCE_STAGE;
    client_context->job_state->percentage=0;
  }
  else {
    sem_wait(&client_context->shuffle_semaphore);
    sem_post(&client_context->shuffle_semaphore);
  }

  // reduce phase
  IntermediateVec* curr_vec;
  size_t shuffled_size = client_context->shuffled_queue.size();
  while ((prev_count = (client_context->atomic_counter->fetch_add(1))) < shuffled_size) {
    client_context->job_state->percentage =
        100.0*client_context->atomic_counter->load()/shuffled_size;
    curr_vec = &(client_context->shuffled_queue[prev_count]);
    client_context->client.reduce(curr_vec, thread_context);
    // TODO reduce uses emit3 to put results into client_context->outputvec
  }

  pthread_exit(NULL);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
  Barrier barrier(multiThreadLevel);

  // define client_context
  ClientContext* client_context = new ClientContext {
    // general
    client, // client
    new JobState {UNDEFINED_STAGE, 0}, // job_state
    pthread_self(), // main_thread
    // vectors
    inputVec, // inputVec
    std::vector<IntermediateVec>(multiThreadLevel), // intermediate_vecs
    std::vector<IntermediateVec>(), // shuffle_queue
    outputVec, // outputVec
    // locks
    new std::atomic<int>(0), // atomic_counter
    &barrier, // barrier
    sem_t(), // shuffle_semaphore
    PTHREAD_MUTEX_INITIALIZER // vector_mutex
  };
  sem_init(&(client_context->shuffle_semaphore), 0, 0);

  // create threads
  pthread_t threads[multiThreadLevel];
  int ret;
  ThreadContext* thread_context;
  for(int i = 0; i < multiThreadLevel; i++) {
    thread_context = new ThreadContext {
      i, // thread_id
      client_context, // client_context
      client_context->intermediate_vecs[i] // intermediate_vec
    };
    ret = pthread_create(&threads[i], NULL, thread_entry_point, thread_context);
    if (ret) {
      exit(-1);
    }
  }

  // wait for all threads to finish
  for (int i = 0; i < multiThreadLevel; ++i) {
    pthread_join(threads[i], NULL);
  }

  int i = *(client_context->atomic_counter);
  return (JobHandle) client_context;
}

void emit2 (K2* key, V2* value, void* context){
    auto* ctx = static_cast<ThreadContext*>(context);
    IntermediatePair element{key, value};
    pthread_mutex_lock(&ctx->client_context->vector_mutex);
    ctx->intermediate_vec.push_back(element);
    pthread_mutex_unlock(&ctx->client_context->vector_mutex);
}

void emit3 (K3* key, V3* value, void* context){
    auto* ctx = static_cast<ThreadContext*>(context);
    OutputPair element{key, value};
    pthread_mutex_lock(&ctx->client_context->vector_mutex);
    ctx->client_context->outputVec.push_back(element);
    pthread_mutex_unlock(&ctx->client_context->vector_mutex);

}