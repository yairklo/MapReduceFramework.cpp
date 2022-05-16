//
// Created by yairklo on 26/04/2022.
//

#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <atomic>
#include <algorithm>
#include <utility>
#include <Barrier.h>
#include <iostream>

bool is_same_k2_key(K2* a, K2* b){return (!(*a < *b) and !(*b < *a));} // are the equal
bool order_relation_K2(K2* a,K2* b){return *a<*b;}
bool order_relation_k2v2(std::pair<K2*,V2*> a,std::pair<K2*,V2*> b){return *a.first<*b.first;}


class MapReduceHandle{
public:
    MapReduceHandle(const MapReduceClient& client,
                    InputVec inputVec, OutputVec& outputVec,
                    int multiThreadLevel): client(client), inputVec(std::move(inputVec)), outputVec(outputVec),
                    mutex(PTHREAD_MUTEX_INITIALIZER),waitForJobMutex(PTHREAD_MUTEX_INITIALIZER),
                    state_mutex(PTHREAD_MUTEX_INITIALIZER), barrier(multiThreadLevel){

        numThreads = multiThreadLevel;

        jobState.stage=UNDEFINED_STAGE;
        jobState.percentage=0;
        atomic_counter = 0;
        vectors_counter = 0;
        atomic_counter_map_pairs = 0;
        is_shuffled = false;

        threads = (pthread_t**) malloc(sizeof(pthread_t*) * multiThreadLevel );
        for (int i = 0; i < multiThreadLevel; ++i) {
            threads[i] = (pthread_t*) malloc(sizeof(pthread_t));
        }

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
    std::atomic<unsigned long> atomic_counter_map_pairs;
    std::atomic<int> vectors_counter;
    std::atomic<int> is_shuffled;

    std::map<K2*, IntermediateVec*> map;
    std::vector<IntermediateVec> intermediateVectors;

    std::vector<K2*> all_keys;
    std::vector<IntermediateVec> shuffled_vec;

    int cc; // todo remove

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
        mapReduceHandle->atomic_counter_map_pairs++;
        mapReduceHandle->jobState.percentage = 100*mapReduceHandle->atomic_counter_map_pairs.load()/(float)mapReduceHandle->inputVec.size(); // each thread that reached here adds 1, not by atomic counter which might make weired things
        for (auto i: *vec)
            mapReduceHandle->all_keys.push_back(i.first);
        pthread_mutex_unlock(&mapReduceHandle->state_mutex);
    }

    // sort phase (by key)
    std::sort(vec->begin(),vec->end(), order_relation_k2v2);

    pthread_mutex_lock(&mapReduceHandle->mutex);
    mapReduceHandle->intermediateVectors.push_back(*vec);
    pthread_mutex_unlock(&mapReduceHandle->mutex);

}
/**
 * give vec<vec<*k2,*v2>> with the inner vec been sorted,output vec<vec<*k2,*v2>> grouped by k2
 * @param context
 */
void shuffle(void * context){
    // all the key values will be use in order to pull from each vector from maximum to minimum key value
    auto * mapReduceHandle = (MapReduceHandle *) context;

    // sort the keys
    std::sort(mapReduceHandle->all_keys.begin(),mapReduceHandle->all_keys.end(), order_relation_K2);


    // for each key from largest to smallest:
    while (!mapReduceHandle->all_keys.empty()){
        // load key (and remove it from key-vector)
        K2 *k = mapReduceHandle->all_keys.back();

        mapReduceHandle->all_keys.pop_back();

        auto vec = new IntermediateVec();
        // for each vector pull all the values with key = current max key.
        for (int i = 0; i < mapReduceHandle->numThreads; ++i) {

        //}
       // for (IntermediateVec vector : mapReduceHandle->intermediateVectors) {

        while (!mapReduceHandle->intermediateVectors[i].empty() && is_same_k2_key(k,mapReduceHandle->intermediateVectors[i].back().first)){
            vec->push_back(mapReduceHandle->intermediateVectors[i].back());
            mapReduceHandle->intermediateVectors[i].pop_back();
           //mapReduceHandle->num_pairs += mapReduceHandle->intermediateVectors[i].size();
            }
        }

        // if we finished with a certain key - push the vector with pairs of it.
        mapReduceHandle->shuffled_vec.push_back(*vec);

        //remove repeated values in sorted all_keys
        while(!mapReduceHandle->all_keys.empty() && is_same_k2_key(k, mapReduceHandle->all_keys.back()))
            mapReduceHandle->all_keys.pop_back();
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
        mapReduceHandle->jobState.percentage = 0;
        mapReduceHandle->jobState.stage = MAP_STAGE;
    }
    pthread_mutex_unlock(&mapReduceHandle->state_mutex);

    // enter map phase, where each thread pull values without repeats, apply map, sort the res and save res to in 'global vector'
    map_phase(mapReduceHandle);

    // wait until all the values where pulled and mapped
    mapReduceHandle->barrier.barrier();

    // make only one thread shuffle
    if (!(mapReduceHandle->is_shuffled++)){
        mapReduceHandle->jobState.percentage = 0;
        mapReduceHandle->jobState.stage = SHUFFLE_STAGE;
        shuffle(mapReduceHandle);
        mapReduceHandle->atomic_counter = 0;
        mapReduceHandle->jobState.stage = REDUCE_STAGE;
    }

    //wait for shuffle to end
    mapReduceHandle->barrier.barrier();

    split_reduce_save(mapReduceHandle);

    //wait for program to end todo kill it
    mapReduceHandle->barrier.barrier();

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
    mapReduceHandle->jobState.percentage += 100/(float)mapReduceHandle->shuffled_vec.size();
    mapReduceHandle->cc++;
    pthread_mutex_unlock(&mapReduceHandle->mutex);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

    auto mapReduceHandle = new MapReduceHandle(client, inputVec, outputVec, multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(mapReduceHandle->threads[i], nullptr, thread_main, mapReduceHandle) != 0){
            exit(1);
        }
    }
    return mapReduceHandle;

}

void waitForJob(JobHandle job){
    auto *mapReduceHandle = (MapReduceHandle*) job;
    pthread_mutex_lock(&mapReduceHandle->waitForJobMutex);
    for (int i = 0; i < mapReduceHandle->numThreads; ++i) pthread_join(*(mapReduceHandle->threads[i]), nullptr);
    mapReduceHandle->atomic_counter = 1;
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