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
    std::map<K2*, IntermediateVec*> map;
    JobState jobState;
    unsigned long long n_values_a_stage;
};


void * run_thread(void * context){
    auto * mapReduceHandle = (MapReduceHandle *) context;
    auto vec = new IntermediateVec();
    while ((*(mapReduceHandle->atomic_counter)).load() < mapReduceHandle->n_values_a_stage){
        int old_value = (*(mapReduceHandle->atomic_counter))++;
        InputPair inputPair = mapReduceHandle->inputVec[old_value];
        mapReduceHandle->client.map(inputPair.first, inputPair.second, vec);
        mapReduceHandle->jobState.percentage = (float)(mapReduceHandle->atomic_counter)->load()/(float)mapReduceHandle->n_values_a_stage;
    }
    std::sort(vec->begin(),vec->end()); //Todo
    auto barrier = new Barrier(mapReduceHandle->numThreads);
    barrier->barrier();



    return nullptr;
}

void emit2 (K2* key, V2* value, void* context){
    auto * vec = (IntermediateVec *) context;
    vec->push_back(IntermediatePair(key,value));
}

void emit3 (K3* key, V3* value, void* context);

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