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
                    int multiThreadLevel): client(client),
                    inputVec(inputVec),
                    outputVec(outputVec),
                                           mutex(PTHREAD_MUTEX_INITIALIZER){
        numThreads = multiThreadLevel;
        threads = (pthread_t**) malloc(sizeof(pthread_t*) * multiThreadLevel );
        for (int i = 0; i < multiThreadLevel; ++i) {
            threads[i] = (pthread_t*) malloc(sizeof(pthread_t));
        }
        n_values_a_stage = inputVec.size();

        jobState.stage=UNDEFINED_STAGE;
        jobState.percentage=0;
        *atomic_counter = 0;
        *is_shuffled = false;
    }

    ~MapReduceHandle(){
        for (int i = 0; i < numThreads; i++) {
            free(threads[i]);
        }
        free(threads);
    }

    pthread_t **threads;
    int numThreads;
    const MapReduceClient& client;
    InputVec inputVec;
    OutputVec& outputVec;
    pthread_mutex_t mutex;
    std::atomic<int>* atomic_counter;
    std::atomic<bool>* is_shuffled;
    std::map<K2*, IntermediateVec*> map;
    std::vector<IntermediateVec> intermediateVec;
    JobState jobState;
    unsigned long long n_values_a_stage;
    std::vector<K2*> all_keys;
    std::vector<IntermediateVec> shuffled_vec;
};

void split_and_map(void * context){
    auto * mapReduceHandle = (MapReduceHandle *) context;
    auto vec = new IntermediateVec();

    // splitting + map
    while ((*(mapReduceHandle->atomic_counter)).load() < mapReduceHandle->n_values_a_stage){
        int old_value = (*(mapReduceHandle->atomic_counter))++;
        InputPair inputPair = mapReduceHandle->inputVec[old_value];
        mapReduceHandle->client.map(inputPair.first, inputPair.second, vec);
        mapReduceHandle->jobState.percentage = (float)(mapReduceHandle->atomic_counter)->load()/(float)mapReduceHandle->n_values_a_stage;
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
    *mapReduceHandle->is_shuffled = true;
    std::sort(mapReduceHandle->all_keys.begin(),mapReduceHandle->all_keys.end());
    mapReduceHandle->all_keys.erase( unique( mapReduceHandle->all_keys.begin(), mapReduceHandle->all_keys.end() ), mapReduceHandle->all_keys.end() );

    while (!mapReduceHandle->intermediateVec.empty()){
        K2 * k = mapReduceHandle->all_keys.back();
        mapReduceHandle->all_keys.pop_back();

        for (IntermediateVec  vector : mapReduceHandle->intermediateVec) {
            if (k == vector.back().first){

            }
        }
    }
}

void split_reduce_save(void *context){
    IntermediateVec intermediateVec;
    auto* mapReduceHandle = (MapReduceHandle*) context;
    while (!mapReduceHandle->shuffled_vec.empty()) {

        // lock the group mutex, st only one vector at a time
        pthread_mutex_lock(&mapReduceHandle->mutex);
        intermediateVec = mapReduceHandle->shuffled_vec.back();
        mapReduceHandle->shuffled_vec.pop_back();
        pthread_mutex_unlock(&mapReduceHandle->mutex);

        mapReduceHandle->client.reduce(&intermediateVec, mapReduceHandle);
    }
}



void * run_thread(void * context){

    auto * mapReduceHandle = (MapReduceHandle *) context;
    split_and_map(mapReduceHandle);
    auto barrier = new Barrier(mapReduceHandle->numThreads);
    barrier->barrier();

    if (!mapReduceHandle->is_shuffled){
        shuffle(mapReduceHandle);
    }

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
    pthread_mutex_unlock(&mapReduceHandle->mutex);
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

}

void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state);
void closeJobHandle(JobHandle job);