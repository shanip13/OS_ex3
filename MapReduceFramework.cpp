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
    const MapReduceClient &client;
    std::atomic<uint64_t>* job_state;
    std::vector<pthread_t> threads;
    // vectors
    const InputVec &inputVec;
    std::vector<IntermediateVec> intermediate_vecs;
    std::vector<IntermediateVec> shuffled_queue;
    size_t intermediate_size;
    OutputVec &outputVec;
    // locks
    std::atomic<bool> *is_waited;
    std::atomic<int> *atomic_counter;
    Barrier *barrier;
    sem_t shuffle_semaphore;
    pthread_mutex_t vector_mutex;
} ClientContext;

typedef struct {
    int thread_id;
    ClientContext *client_context;
    IntermediateVec &intermediate_vec;
} ThreadContext;


void waitForJob(JobHandle job) {
    if (job == nullptr) {
        return;
    }
    auto *client_context = static_cast<ClientContext *>(job);
    if (!client_context->is_waited->exchange(true)) {
      for (auto &thread : client_context->threads) {
        int ret = pthread_join (thread, nullptr);
        if (ret != 0)
        {
          std::cout << "system error: failed in joining job thread.\n";
          exit(1);
        }
      }
    }
}

void change_phase(ClientContext* client_context, uint64_t new_stage, uint64_t phase_size) {
  uint64_t new_state = (((uint64_t) new_stage) << 62)
                     + (((uint64_t) phase_size) << 31);
  client_context->job_state->store(new_state);
}

void progress_state_by_num(ClientContext* client_context, size_t to_add) {
  client_context->job_state->fetch_add(to_add & (0x7FFFFFFF));
}

void getJobState(JobHandle job, JobState *state) {
  auto *client_context = static_cast<ClientContext *>(job);

  uint64_t job_stage = client_context->job_state->load();
  stage_t stage = (stage_t) ((client_context->job_state->load()) >> 62);
  float finished = job_stage & (0x7FFFFFFF);
  float all = (job_stage >> 31) & (0x7FFFFFFF);
  state->percentage = 100.0*finished/all;
  state->stage = stage;
}

void closeJobHandle(JobHandle job) {
    if (job == nullptr) {
        return;
    }
    auto *context = static_cast<ClientContext *>(job);

  waitForJob(job);
    if (sem_destroy(&context->shuffle_semaphore) != 0) {
      std::cout << "system error: failed to destroy semaphore\n";
      exit(1);
    }
    delete context->atomic_counter;
    delete context;
}


bool compareIntermediatePair(const IntermediatePair &pair1, const IntermediatePair &pair2) {
    return pair1.first < pair2.first;
}

bool K2_equals(K2 *key1, K2 *key2) {
    return (!(*key1 < *key2)) && (!(*key2 < *key1));
}

int shuffle(ClientContext *client_context) {
    std::vector<IntermediateVec> &intermediate_vecs = client_context->intermediate_vecs;
    std::vector<IntermediateVec> &shuffled_queue = client_context->shuffled_queue;

    while (!intermediate_vecs.empty()) {
        // delete empty vectors
        auto it = intermediate_vecs.begin();
        while (it != intermediate_vecs.end()) {
            if (it->empty()) { it = intermediate_vecs.erase(it); }
            else { ++it; }
        }
        if (intermediate_vecs.empty()) { break; }

        // find the largest key
        K2 *curr_key;
        K2 *largest_key = intermediate_vecs[0].back().first;
        for (auto &vec: intermediate_vecs) {
            curr_key = vec.back().first;
            if (*largest_key < *curr_key) {
                largest_key = curr_key;
            }
        }

        // pop elements with the largest key into new sequence
        shuffled_queue.push_back(*(new IntermediateVec()));
        for (auto &vec: intermediate_vecs) {
            while (!vec.empty() && K2_equals(vec.back().first, largest_key)) {
                shuffled_queue.back().push_back(vec.back());
                vec.pop_back();
                progress_state_by_num(client_context, 1);
            }
        }
    }
    return 0;
}

void *thread_entry_point(void *arg) {
    ThreadContext *thread_context = (ThreadContext *) arg;
    ClientContext *client_context = thread_context->client_context;

    // map phase
    size_t input_size = client_context->inputVec.size();
    const InputPair *curr_pair;
    int prev_count;
    while ((prev_count = (client_context->atomic_counter->fetch_add(1))) < input_size) {
      // map element
      curr_pair = &(client_context->inputVec[prev_count]);
      client_context->client.map(curr_pair->first, curr_pair->second,
                                   thread_context);
      progress_state_by_num(client_context, 1);
    }

    // sort phase
    std::sort(thread_context->intermediate_vec.begin(),
              thread_context->intermediate_vec.end(),
              compareIntermediatePair);

    // barrier
    client_context->barrier->barrier();

    // shuffle phase
    if (thread_context->thread_id == 0) {
        // get num of intermediate elements
        size_t intermediate_size = 0;
        for (const auto &vec: client_context->intermediate_vecs) {
          intermediate_size += vec.size();
        }
        client_context->intermediate_size = intermediate_size;
        change_phase(client_context, SHUFFLE_STAGE, intermediate_size);
        shuffle(client_context);
        client_context->atomic_counter->store(0);
        change_phase(client_context, REDUCE_STAGE, intermediate_size);
        sem_post(&client_context->shuffle_semaphore);
    } else {
        sem_wait(&client_context->shuffle_semaphore);
        sem_post(&client_context->shuffle_semaphore);
    }

    // reduce phase
    IntermediateVec *curr_vec;
    size_t intermediate_size = client_context->intermediate_size;
    size_t num_vectors = client_context->shuffled_queue.size();
    while ((prev_count = (client_context->atomic_counter->fetch_add(1))) < num_vectors) {
      curr_vec = &(client_context->shuffled_queue[prev_count]);
      client_context->client.reduce(curr_vec, thread_context);
      progress_state_by_num(client_context, client_context->shuffled_queue[prev_count].size());
    }

    pthread_exit(NULL);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    Barrier* barrier = new Barrier(multiThreadLevel);
    // define client_context
    ClientContext *client_context = new ClientContext{
      // general
      client, // client
      new std::atomic<uint64_t>(0), // job_state
      std::vector<pthread_t>(multiThreadLevel), // threads
      // vectors
      inputVec, // inputVec
      std::vector<IntermediateVec>(multiThreadLevel), // intermediate_vecs
      std::vector<IntermediateVec>(), // shuffle_queue
      0, // intermediate_size
      outputVec, // outputVec
      // locks
      new std::atomic<bool>(false), // is_waited
      new std::atomic<int>(0), // atomic_counter
      barrier, // barrier
      sem_t(), // shuffle_semaphore
      PTHREAD_MUTEX_INITIALIZER // vector_mutex
    };
    if(sem_init(&(client_context->shuffle_semaphore), 0, 0)!=0){
      std::cout<<"system error: failed in semaphore init\n";
      exit(1);
    }

    // create threads
    int ret;
    ThreadContext *thread_context;
    size_t input_size = inputVec.size();
    change_phase(client_context, MAP_STAGE, input_size);
    for (int i = 0; i < multiThreadLevel; i++) {
        thread_context = new ThreadContext{
          i, // thread_id
          client_context, // client_context
          client_context->intermediate_vecs[i] // intermediate_vec
        };
        ret = pthread_create(&client_context->threads[i], NULL, thread_entry_point,
                             thread_context);
        if (ret != 0) {
            std::cout << "system error: failed creating pthread\n";
            exit(1);
        }
    }

    return (JobHandle) client_context;
}

void emit2(K2 *key, V2 *value, void *context) {
    auto *ctx = static_cast<ThreadContext *>(context);
    IntermediatePair element{key, value};
    int ret;
    ret = pthread_mutex_lock(&ctx->client_context->vector_mutex);
    if (ret != 0) {
      std::cout << "system error: failed in mutex_lock\n";
      exit(1);
    }
    ctx->intermediate_vec.push_back(element);
    ret = pthread_mutex_unlock(&ctx->client_context->vector_mutex);
    if (ret != 0) {
      std::cout << "system error: failed in mutex_unlock\n";
      exit(1);
    }
}

void emit3(K3 *key, V3 *value, void *context) {
    auto *ctx = static_cast<ThreadContext *>(context);
    OutputPair element{key, value};
    int ret;
    ret = pthread_mutex_lock(&ctx->client_context->vector_mutex);
    if(ret !=0){
      std::cout<<"system error: failed in mutex_lock\n";
      exit(1);
    }
    ctx->client_context->outputVec.push_back(element);
    ret = pthread_mutex_unlock(&ctx->client_context->vector_mutex);
    if(ret !=0){
      std::cout<<"system error: failed in mutex_unlock\n";
      exit(1);
    }

}