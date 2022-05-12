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


// todo remove

class KChar : public K2, public K3 {
public:
    KChar(char c) : c(c) {}

    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    char c;
};
// todo end to remove

class MapReduceHandle{
public:
    MapReduceHandle(const MapReduceClient& client,
                    const InputVec& inputVec, OutputVec& outputVec,
                    int multiThreadLevel): client(client), inputVec(inputVec), outputVec(outputVec),
                    mutex(PTHREAD_MUTEX_INITIALIZER),waitForJobMutex(PTHREAD_MUTEX_INITIALIZER),
                    state_mutex(PTHREAD_MUTEX_INITIALIZER), barrier(multiThreadLevel){

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

    Barrier barrier;

    pthread_mutex_t mutex;
    pthread_mutex_t state_mutex;
    pthread_mutex_t waitForJobMutex;

    std::atomic<unsigned long> atomic_counter;
    std::atomic<int> vectors_counter;
    std::atomic<int> is_shuffled;

    std::map<K2*, IntermediateVec*> map;
    std::vector<IntermediateVec> intermediateVec;

    std::vector<K2*> all_keys;
    std::vector<IntermediateVec> shuffled_vec;

    JobState jobState;
    unsigned long num_pairs;
};

void map_phase(void * context){
    auto * mapReduceHandle = (MapReduceHandle *) context;
    auto vec = new IntermediateVec();

    //
    unsigned long old_value;
    while (true){
        old_value = mapReduceHandle->atomic_counter++;
        if(old_value >= mapReduceHandle->inputVec.size())
            break;

        InputPair inputPair = mapReduceHandle->inputVec[old_value];

        // after mapping immediately update state, and place vec in context's vector all_keys without resource race
        pthread_mutex_lock(&mapReduceHandle->state_mutex);
        mapReduceHandle->client.map(inputPair.first, inputPair.second, vec);
        mapReduceHandle->jobState.percentage += 100/(float)mapReduceHandle->inputVec.size(); // each thread thattht reached here adds 1, not by atomic counter which might make weired things
        mapReduceHandle->all_keys.push_back(vec->back().first);
        pthread_mutex_unlock(&mapReduceHandle->state_mutex);
    }

    // sort
    std::sort(vec->begin(),vec->end()); //Todo

    pthread_mutex_lock(&mapReduceHandle->mutex);
    mapReduceHandle->intermediateVec.push_back(*vec);
    pthread_mutex_unlock(&mapReduceHandle->mutex);

}

void shuffle(void * context){
    auto * mapReduceHandle = (MapReduceHandle *) context;

    std::sort(mapReduceHandle->all_keys.begin(),mapReduceHandle->all_keys.end());
    mapReduceHandle->all_keys.erase( unique( mapReduceHandle->all_keys.begin(), mapReduceHandle->all_keys.end() ), mapReduceHandle->all_keys.end() );


    while (!mapReduceHandle->all_keys.empty()){ // again, no resource race
        K2 * k = mapReduceHandle->all_keys.back();
        mapReduceHandle->all_keys.pop_back();

        auto vec = new IntermediateVec();
        for (IntermediateVec vector : mapReduceHandle->intermediateVec) {

            while (!vector.empty() && k == vector.back().first){ // no need for mutex as only one thread is here
                vec->push_back(vector.back());
                vector.pop_back();
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

    unsigned long old_value;
    while (true) {
        old_value = mapReduceHandle->vectors_counter++;
        if (old_value >= mapReduceHandle->shuffled_vec.size()) // sanity: avoid resources race.
            break;
        intermediateVec = mapReduceHandle->shuffled_vec[old_value];
        if(!intermediateVec.empty()) {
            mapReduceHandle->client.reduce(&intermediateVec, mapReduceHandle);
        }
    }
}
/**
 * the program each thread follows.
 * @param context
 * @return
 */
void * thread_main(void * context){

    auto * mapReduceHandle = (MapReduceHandle *) context;

    // update stage
    pthread_mutex_lock(&mapReduceHandle->state_mutex);
    if (mapReduceHandle->jobState.stage == UNDEFINED_STAGE){
        mapReduceHandle->jobState.stage = MAP_STAGE;
        mapReduceHandle->jobState.percentage = 0;
    }
    pthread_mutex_unlock(&mapReduceHandle->state_mutex);

    // enter map phase, where each thread pull values without repeats, apply map, sort the res and save res to in 'global vector'
    map_phase(mapReduceHandle);

    // wait until all the values where pulled and mapped
    mapReduceHandle->barrier.barrier();

    // make only one thread shuffle
    if (!(mapReduceHandle->is_shuffled++)){
        mapReduceHandle->jobState.stage = SHUFFLE_STAGE;
        shuffle(mapReduceHandle);
        mapReduceHandle->jobState.stage = REDUCE_STAGE;
        mapReduceHandle->atomic_counter = 0;
    }

    //wait for shuffle to end
    mapReduceHandle->barrier.barrier();

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
    mapReduceHandle->jobState.percentage += 100/(float)mapReduceHandle->num_pairs;
    pthread_mutex_unlock(&mapReduceHandle->mutex);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    auto mapReduceHandle = new MapReduceHandle(client, inputVec, outputVec, multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(mapReduceHandle->threads[i], NULL, thread_main, mapReduceHandle) != 0){
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
