#include <iostream>
#include <atomic>
#include "MapReduceClient.h"
using namespace std;

struct ThreadContext{
    int threadId;
    std::atomic<int>& actionsCounter;
    const InputVec& inputVec;
    OutputVec& outputVec;
    IntermediateVec& interMidVec;
    const MapReduceClient& client;

};

// func decelerations:
// ---------------
void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);
void* action(void* arg);
// ---------------

void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){

    // Spawn threads,
    // Create a threadPool array
    // And init contexts:
    printf("Creating %d threads \n", multiThreadLevel);
    pthread_t threads[multiThreadLevel];
    vector<ThreadContext> contexts;
    vector<IntermediateVec> allVec(multiThreadLevel); // vector of vectors
    std::atomic<int> atomic_counter(0);

    for (int i = 0; i < multiThreadLevel; i++) {
        contexts.push_back(ThreadContext{i, atomic_counter, inputVec,
                                         outputVec,
                      allVec[i], client});
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_create(&threads[i], nullptr, action, &contexts[i]);
    }

    // Join threads:
    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_join(threads[i], nullptr);
    }
    printf("Completed joining %d threads \n", multiThreadLevel);

}

void emit2 (K2* key, V2* value, void* context){
    ThreadContext* thCtx = (ThreadContext*)context;

    IntermediateVec& vec = thCtx->interMidVec;
    IntermediatePair pair = {key, value};

    // Vec copies pair by value
    vec.push_back(pair);
}

void* action(void* arg){

    ThreadContext* thCtx = (ThreadContext*)arg;

    // Increment val
    int oldVal = (thCtx->actionsCounter)++;

    // Get pair of oldVal and map it
    const InputPair& pair = thCtx->inputVec[oldVal];
    const MapReduceClient& client = thCtx->client;

    client.map(pair.first, pair.second, thCtx);

    pthread_exit(nullptr);
}