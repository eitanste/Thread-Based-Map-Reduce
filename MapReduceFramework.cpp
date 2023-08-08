#include <utility>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <iostream>
#include <mutex>
#include <algorithm>
#include "MapReduceFramework.h"
#include "Barrier.h"

using namespace std;

class Atomic {
public:
    std::atomic<uint64_t> counter;

    Atomic() : counter(0) {}

    // Functions to manipulate the job state
    void setStage(stage_t stage, uint64_t totalKeys) {
        if (totalKeys) {
            counter.exchange(
                    (static_cast<uint64_t>(stage) << 31) + (totalKeys << 33));
        } else {
            auto tmp_val = counter.load();
            counter.exchange(
                    (static_cast<uint64_t>(stage) << 31) +
                    (tmp_val & 0xFFFFFFFE00000000));
        }
    }

    void addProcessedKeys(uint64_t processedKeys) {
        auto tmp = getData();
        if ((tmp & 0x7FFFFFFF) == ((tmp >> 33) & 0x7FFFFFFF) - 1) {
            counter += 2 + processedKeys;
        } else {
            counter += processedKeys;
        }
    }

    uint64_t getData() {
        uint64_t tmp = counter.load();
        return tmp;
    }

};

struct JobContext;

typedef struct ThreadContext {
    int thread_id;
    JobContext *job_context;
    IntermediateVec *intermediate_vec;
} ThreadContext;

typedef struct JobContext {
//    JobState state;
    int num_of_threads;
    Atomic *mutual_atomic_counter;
    const MapReduceClient *client;
    InputVec input_vec;
    OutputVec *output_vec;
    vector<IntermediateVec *> after_map_vec;
    vector<IntermediateVec *> after_shuffle_vec;
    ThreadContext **thread_contexts;
    pthread_t *threads;
    Barrier *barrier;
    bool waiting_for_completion = false;
    pthread_mutex_t wait_for_job_mutex;
    pthread_mutex_t job_mutex;
} JobContext;

void release_memory(JobContext *job) {
    if (job != nullptr) {
        if (job->thread_contexts != nullptr) {
            for (int i = 0; i < job->num_of_threads; i++) {
                // need to dell?
                job->thread_contexts[i]->intermediate_vec = nullptr; // TODO:
                job->thread_contexts[i]->intermediate_vec = nullptr; // TODO:
                // need to dell?
                job->thread_contexts[i]->job_context = nullptr; // TODO:
                delete job->thread_contexts[i];
            }
            delete[] job->thread_contexts;
        }
        if (job->threads != nullptr) {
            delete[] job->threads;
        }
        delete job->mutual_atomic_counter;
        if (pthread_mutex_destroy(&(job->wait_for_job_mutex)) != 0) {
            cout << "Error on pthread_mutex_destroy";
            exit(1);
        }
        if (pthread_mutex_destroy(&(job->job_mutex)) != 0) {
            cout << "Error on pthread_mutex_destroy";
            exit(1);
        }
        delete job->barrier;
        delete job;
    }
}

void error_func(int exit_type, const string &error_msg, JobContext *job) {
    cout << error_msg << endl;
    if (exit_type != 666) {
        release_memory(job);
        exit(exit_type);
    }
}

void check_pointer(void *ptr, const string &error_msg, JobContext *job) {
    if (ptr == nullptr) {
        // TODO: free space
        error_func(EXIT_FAILURE, error_msg, job);
    }
}

void block_job(JobContext *job, bool is_wait_func) {
    pthread_mutex_t *mutex;
    if (is_wait_func) {
        mutex = &(job->wait_for_job_mutex);
    } else {
        mutex = &(job->job_mutex);
    }
    if (pthread_mutex_lock(mutex) != 0) {
        error_func(1, "Error on pthread_mutex_lock in block_job",
                   job);
    }
}

void release_job(JobContext *job, bool is_wait_func) {
    pthread_mutex_t *mutex;
    if (is_wait_func) {
        mutex = &(job->wait_for_job_mutex);
    } else {
        mutex = &(job->job_mutex);
    }
    if (pthread_mutex_unlock(mutex) != 0) {
        error_func(1, "Error on pthread_mutex_unlock in release_job",
                   job);
    }
}

ThreadContext *init_thread_context(int i, JobContext *job) {
    auto *threadContext = new(nothrow) ThreadContext();
    check_pointer(threadContext, "Could not allocate the memory for thread",
                  job);
    threadContext->thread_id = i;
    threadContext->job_context = job;
    return threadContext;
}

void emit2(K2 *key, V2 *value, void *context) {
    auto *thread_context = static_cast<ThreadContext *> (context);
    thread_context->intermediate_vec->push_back(std::make_pair(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    JobContext *job_context = static_cast<JobContext *>(context);
    job_context->output_vec->push_back(std::make_pair(key, value));
}

bool is_equal(const K2 *first, const K2 *second) {
    return !(*first < *second) && !(*second < *first);
}

void concatenate_all_pairs(
        JobContext *job, vector<IntermediatePair> *all_keys_vec) {
    unsigned long len = job->after_map_vec.size();
    for (int i = len - 1; i >= 0; i--) {
        IntermediateVec *in_vec = job->after_map_vec[i];
        unsigned long in_len = in_vec->size();
        for (int j = in_len - 1; j >= 0; --j) {
            all_keys_vec->push_back((*in_vec)[j]);
        }
        delete job->after_map_vec[i];
    }
}

void extract_vecs_by_key(JobContext *job,
                         vector<IntermediatePair> *all_keys_vec) {
    while (!all_keys_vec->empty()) {
        K2 *max_key = all_keys_vec->back().first;
        auto same_keys_vec = new(nothrow) IntermediateVec();
        // TODO: do we need to dell it? or because it is in a vector his
        //  doing it for us?
        check_pointer(same_keys_vec, "Problem allocating memory", job);
        while (!all_keys_vec->empty()
               && is_equal(all_keys_vec->back().first, max_key)) {
            same_keys_vec->push_back(all_keys_vec->back());
            all_keys_vec->pop_back();
            job->mutual_atomic_counter->addProcessedKeys(1);
        }
        job->after_shuffle_vec.push_back(same_keys_vec);
    }
}

void shuffle(JobContext *job) {
    vector<IntermediatePair> all_keys_vec = vector<IntermediatePair>();
    concatenate_all_pairs(job, &all_keys_vec);
    sort(all_keys_vec.begin(), all_keys_vec.end(),
         [](const IntermediatePair &a, const IntermediatePair &b) {
             return *(a.first) < *(b.first);
         });
    job->mutual_atomic_counter->setStage(SHUFFLE_STAGE, all_keys_vec.size());

    extract_vecs_by_key(job, &all_keys_vec);
}

void map_and_sort(ThreadContext *thread_context, JobContext *job_context) {
    InputPair pair_for_map;
    while (true) {
        block_job(job_context, false);
        if (job_context->input_vec.empty()) {
            release_job(job_context, false);
            break;
        } else {
            thread_context->intermediate_vec = new(nothrow) IntermediateVec();
            check_pointer(thread_context->intermediate_vec,
                          "Failed to allocate vector on heap",
                          job_context);
            pair_for_map = job_context->input_vec.back();
            job_context->input_vec.pop_back();
            job_context->client->map(pair_for_map.first, pair_for_map.second,
                                     thread_context);
            release_job(job_context, false);
            sort(thread_context->intermediate_vec->begin(),
                 thread_context->intermediate_vec->end(),
                 [](const IntermediatePair &a, const IntermediatePair &b) {
                     return *(a.first) < *(b.first);
                 });
            block_job(job_context, false);
            job_context->after_map_vec.push_back(
                    thread_context->intermediate_vec);
            job_context->mutual_atomic_counter->addProcessedKeys(1);
            release_job(job_context, false);
        }
    }
}

void reduce(ThreadContext *thread_context, JobContext *job_context) {
    bool done = false;

    while (!done) {
        block_job(job_context, false);

        if (!job_context->after_shuffle_vec.empty()) {
            IntermediateVec *reduce_input = job_context->after_shuffle_vec
                    .back();
            job_context->after_shuffle_vec.pop_back();
            auto input_vec_len = reduce_input->size();

            //release_job (job_context, false, tmp);
            job_context->client->reduce(reduce_input,
                                        job_context);
            //block_job (job_context, false, tmp);

            delete reduce_input;

            job_context->mutual_atomic_counter->addProcessedKeys
                    (input_vec_len);
        } else {
            done = true;
        }

        release_job(job_context, false);
    }
}

void map_reduce_entry_point(ThreadContext *thread_context) {

    if (thread_context->job_context == nullptr) {
        return;
    }


    int thread_id = thread_context->thread_id;

    map_and_sort(thread_context, thread_context->job_context);

    thread_context->job_context->barrier->barrier();

    // Shuffle phase
    if (thread_id == 0) {
        shuffle(thread_context->job_context);
        thread_context->job_context->mutual_atomic_counter->setStage(
                REDUCE_STAGE, 0);
    }

    // Barrier synchronization if thread_id is not 0
    thread_context->job_context->barrier->barrier();

    // Reduce phase
    reduce(thread_context, thread_context->job_context);
}

void init_job(const MapReduceClient &client, const InputVec &inputVec,
              OutputVec *outputVec, int multiThreadLevel,
              JobContext *job) {
    job->thread_contexts = new ThreadContext *[multiThreadLevel];
    check_pointer(job->thread_contexts, "Failed to allocate memory for "
                                        "thread_contexts", job);
    job->threads = new pthread_t[multiThreadLevel];
    check_pointer(job->threads, "Failed to allocate memory for "
                                "threads", job);
    job->num_of_threads = multiThreadLevel;
    auto tmp_atomic = new(nothrow) Atomic();
    check_pointer(tmp_atomic, "Could not allocate memory for atomic param",
                  job);
    job->mutual_atomic_counter = tmp_atomic;
    job->mutual_atomic_counter->setStage(UNDEFINED_STAGE, 0);
    job->wait_for_job_mutex = PTHREAD_MUTEX_INITIALIZER;
    job->job_mutex = PTHREAD_MUTEX_INITIALIZER;
    job->client = &client; // TODO: change type
    job->input_vec = inputVec;
    job->output_vec = outputVec;
    job->barrier = new(nothrow) Barrier(multiThreadLevel);
    check_pointer(job->barrier, "Could not allocate memory for barrier", job);
}


JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    // TODO: check all args
    if (multiThreadLevel <= 0) {
        error_func(EXIT_FAILURE, "Invalid multiThreadLevel", nullptr);
    }
    if (inputVec.empty() || !outputVec.empty()) {
        error_func(EXIT_FAILURE, "Invalid input vector", nullptr);
    }
    auto *job = new(nothrow) JobContext();
    check_pointer(job, "Could not allocate memory for job context", job);

    init_job(client, inputVec, &outputVec, multiThreadLevel, job);

    job->mutual_atomic_counter->setStage(MAP_STAGE, job->input_vec.size());

    for (int i = 0; i < multiThreadLevel; i++) {
        // init context
        job->thread_contexts[i] = init_thread_context(i, job);
        if (job->thread_contexts[i]->thread_id != i) {
            cout << "thread did not allocated thread context properly" << endl;
        }
        if (pthread_create(&job->threads[i], nullptr,
                           reinterpret_cast<void *(*)(void *)>
                           (map_reduce_entry_point),  // TODO: WTF is this cast?
                           job->thread_contexts[i]) != 0) {
            error_func(1, "Couldn't create the thread", job);
        }
    }
    return static_cast<JobHandle> (job);
}

void waitForJob(JobHandle job) {
    auto *job_ctx = static_cast<JobContext *> (job);
    if (job == nullptr) {
        error_func(666, "Job pointer could not be empty. Returning without "
                        "executing", job_ctx);  // TODO: check if this
        // behaviour is  correct
        return;
    }


    block_job(job_ctx, true);

    block_job(job_ctx, false);

    auto val = job_ctx->waiting_for_completion;
    job_ctx->waiting_for_completion = true;

    release_job(job_ctx, false);

    if (!val) {
        for (int i = 0; i < job_ctx->num_of_threads; ++i) {
            pthread_join(job_ctx->threads[i], NULL);
        }
    }

    release_job(job_ctx, true);
}

void getJobState(JobHandle job, JobState *state) {
//    *state = ((JobContext *) job)->state; // TODO: protect with mutex
    auto job_context = static_cast<JobContext *>(job);
    if (job == nullptr) {
        error_func(666, "Job pointer could not be empty. Returning without "
                        "executing", job_context);  // TODO: check if this
        // behaviour is correct
        return;
    }
    auto state_value = job_context->mutual_atomic_counter->getData();
    state->stage = static_cast<stage_t> ((state_value >> 31) & 0x3);
    auto total_keys = ((state_value >> 33) & 0x7FFFFFFF);
    auto processed_keys = (state_value & 0x7FFFFFFF);
    if (total_keys == 0) {
        state->percentage = 0.0;
        return;
    }
    auto percentage = processed_keys / (total_keys / 100.0);
    state->percentage = percentage < 100.0 ? percentage : 100.0;
}

void closeJobHandle(JobHandle job) {
    auto *job_ctx = static_cast<JobContext *> (job);
    if (job == nullptr) {
        error_func(666, "Job pointer could not be empty. Returning without "
                        "executing", job_ctx);  // TODO: check if this
        // behaviour is correct
        return;
    }
    waitForJob(job_ctx);
    release_memory(job_ctx);
}

