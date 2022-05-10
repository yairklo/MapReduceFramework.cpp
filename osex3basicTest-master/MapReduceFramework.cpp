//
// Created by yairklo on 26/04/2022.
//

#include "MapReduceFramework.h"
#include <pthread.h>
#include <iostream>
#include <map>
#include <atomic>
#include <algorithm>
#include <Barrier.h>


class MapReduceHandle{
public:
    MapReduceHandle(const MapReduceClient& client,
                    const InputVec& inputVec, OutputVec& outputVec,
                    int multiThreadLevel): client(client), inputVec(inputVec), outputVec(outputVec),
                    mutex(PTHREAD_MUTEX_INITIALIZER),waitForJobMutex(PTHREAD_MUTEX_INITIALIZER),
                    state_mutex(PTHREAD_MUTEX_INITIALIZER){

        numThreads = multiThreadLevel;
        threads = (pthread_t**) malloc(sizeof(pthread_t*) * multiThreadLevel );
        for (int i = 0; i < multiThreadLevel; ++i) {
            threads[i] = (pthread_t*) malloc(sizeof(pthread_t));
        }

        jobState.stage=UNDEFINED_STAGE;
        jobState.percentage=0;

        atomic_counter = 0;
        vectors_counter = 0;

        is_shuffled = false;
    }

    ~MapReduceHandle(){
        for (int i = 0; i < numThreads; i++) {
            free(threads[i]);
        }
        free(threads);

        pthread_mutex_destroy(&mutex);
        pthread_mutex_destroy(&waitForJobMutex);
        pthread_mutex_destroy(&state_mutex);
    }

    pthread_t **threads;
    int numThreads;
    const MapReduceClient& client;
    InputVec inputVec;
    OutputVec& outputVec;

    pthread_mutex_t mutex;
    pthread_mutex_t state_mutex;
    pthread_mutex_t waitForJobMutex;

    std::atomic<int> atomic_counter;
    std::atomic<int> vectors_counter;
    std::atomic<bool> is_shuffled;

    std::map<K2*, IntermediateVec*> map;
    std::vector<IntermediateVec> intermediateVec;

    std::vector<K2*> all_keys;
    std::vector<IntermediateVec> shuffled_vec;

    JobState jobState;
    unsigned long num_pairs;
};

void thread_main(void * context){
    auto * mapReduceHandle = (MapReduceHandle *) context;
    auto vec = new IntermediateVec();

    // splitting + map
    int old_value;
    while ((old_value = mapReduceHandle->atomic_counter++) < mapReduceHandle->inputVec.size()){
        InputPair inputPair = mapReduceHandle->inputVec[old_value];

        pthread_mutex_lock(&mapReduceHandle->state_mutex); // after mapping immediately update.
        mapReduceHandle->client.map(inputPair.first, inputPair.second, vec);
        mapReduceHandle->jobState.percentage = 100*(float)(mapReduceHandle->atomic_counter).load()/(float)mapReduceHandle->inputVec.size();
        pthread_mutex_unlock(&mapReduceHandle->state_mutex);

        mapReduceHandle->all_keys.push_back(vec->back().first);
    }

    // sort
    std::sort(vec->begin(),vec->end()); //Todo

    pthread_mutex_lock(&mapReduceHandle->mutex);
    mapReduceHandle->intermediateVec.push_back(*vec);
    pthread_mutex_unlock(&mapReduceHandle->mutex);

}

void shuffle(void * context){
    auto * mapReduceHandle = (MapReduceHandle *) context;
    mapReduceHandle->is_shuffled = true;

    pthread_mutex_lock(&mapReduceHandle->state_mutex);
    mapReduceHandle->jobState.stage = SHUFFLE_STAGE;
    mapReduceHandle->jobState.percentage = 0;
    pthread_mutex_unlock(&mapReduceHandle->state_mutex);

    std::sort(mapReduceHandle->all_keys.begin(),mapReduceHandle->all_keys.end());
    mapReduceHandle->all_keys.erase( unique( mapReduceHandle->all_keys.begin(), mapReduceHandle->all_keys.end() ), mapReduceHandle->all_keys.end() );


    while (!mapReduceHandle->all_keys.empty()){ // only one thread here => no resource race, and it's kind
        K2 * k = mapReduceHandle->all_keys.back();
        mapReduceHandle->all_keys.pop_back();

        auto vec = new IntermediateVec();
        for (IntermediateVec  vector : mapReduceHandle->intermediateVec) {

            while (k == vector.back().first){ // no need for mutex as only one thread is here
                vec->push_back(vector.back());
                vector.pop_back();
                if (vector.empty()){
                    break;
                }
            }
        }
        mapReduceHandle->shuffled_vec.push_back(*vec);
    }
    for (const IntermediateVec& vector: mapReduceHandle->shuffled_vec){
        mapReduceHandle->num_pairs += vector.size();
    }
}

void split_reduce_save(void *context){
    IntermediateVec intermediateVec;
    auto* mapReduceHandle = (MapReduceHandle*) context;

    pthread_mutex_lock(&mapReduceHandle->state_mutex);
    mapReduceHandle->jobState.stage = REDUCE_STAGE;
    mapReduceHandle->jobState.percentage = 0;
    pthread_mutex_unlock(&mapReduceHandle->state_mutex);

    int old_value;
    while ((old_value=mapReduceHandle->vectors_counter++) < mapReduceHandle->shuffled_vec.size()){ // to avoid resource race problems
        intermediateVec = mapReduceHandle->shuffled_vec[old_value];
        mapReduceHandle->client.reduce(&intermediateVec, mapReduceHandle);
    }
}

void * run_thread(void * context){

    auto * mapReduceHandle = (MapReduceHandle *) context;

    pthread_mutex_lock(&mapReduceHandle->state_mutex);
    mapReduceHandle->jobState.stage = MAP_STAGE;
    mapReduceHandle->jobState.percentage = 0;
    pthread_mutex_unlock(&mapReduceHandle->state_mutex);

    thread_main(mapReduceHandle);
    auto barrier = new Barrier(mapReduceHandle->numThreads);
    barrier->barrier();

    if (!mapReduceHandle->is_shuffled){
        shuffle(mapReduceHandle);
    }
    mapReduceHandle->atomic_counter = 0;
    split_reduce_save(mapReduceHandle);
    return nullptr;
}

void emit2 (K2* key, V2* value, void* context){
    auto * vec = (IntermediateVec *) context;
    vec->push_back(IntermediatePair(key,value));
}

void emit3 (K3* key, V3* value, void* context){
    auto* mapReduceHandle = (MapReduceHandle*) context;
    std::pair<K3*,V3*> item {key,value};

    pthread_mutex_lock(&mapReduceHandle->mutex);
    mapReduceHandle->outputVec.push_back(item);
    mapReduceHandle->atomic_counter++;
    pthread_mutex_unlock(&mapReduceHandle->mutex);

    pthread_mutex_lock(&mapReduceHandle->state_mutex);
    mapReduceHandle->jobState.percentage = 100*(float)(mapReduceHandle->atomic_counter).load()/(float)mapReduceHandle->num_pairs;
    pthread_mutex_unlock(&mapReduceHandle->state_mutex);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    auto mapReduceHandle = new MapReduceHandle(client, inputVec, outputVec, multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(mapReduceHandle->threads[i], NULL, run_thread, mapReduceHandle) != 0){
            exit(1);
        }
    }
    return mapReduceHandle;

}

void waitForJob(JobHandle job){
    auto *mapReduceHandle = (MapReduceHandle*) job;
    pthread_mutex_lock(&mapReduceHandle->waitForJobMutex);
    for (int i = 0; i < mapReduceHandle->numThreads; ++i) pthread_join(*(mapReduceHandle->threads[i]), nullptr);
    pthread_mutex_unlock(&mapReduceHandle->waitForJobMutex);
}

void getJobState(JobHandle job, JobState* state){// add mutex
    auto *mapReduceHandle = (MapReduceHandle*) job;

    pthread_mutex_lock(&mapReduceHandle->state_mutex);
    *state = mapReduceHandle->jobState;
    pthread_mutex_unlock(&mapReduceHandle->state_mutex);
}


void closeJobHandle(JobHandle job){
    waitForJob(job);
    auto mapReduceHandle = (MapReduceHandle*) job;
    delete mapReduceHandle;
}
