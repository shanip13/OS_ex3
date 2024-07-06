#include "MapReduceFramework.h"
#include <atomic>

typedef struct {
    JobState job_state;
    const InputVec& inputVec; // TODO maybe change &
    OutputVec& outputVec;
    std::atomic<int>* atomic_counter;
} ClientContext;

typedef struct {
    ClientContext& client_context;
    IntermediateVec& intermediate_vec;
} ThreadContext;