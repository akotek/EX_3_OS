#include <iostream>
#include <atomic>
#include "MapReduceClient.h"
using namespace std;


// a Thread structure
class ThreadContext{
public:

    int tId;
    std::atomic<int>* actionsCounter;
    vector<InputPair>& inputPairPtr;

private:
};

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);
void* action(void* arg);


void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){


    // Spawn threads:
    printf("Creating %d threads \n", multiThreadLevel);
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    vector< vector<InputPair> > thInputPairs; // Each vector reps a Thread
    std::atomic<int> atomic_counter(0); // shared variable

    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] = {i, &atomic_counter, thInputPairs[i]};

    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, nullptr, action, contexts + i);
    }

    // Join threads:
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], nullptr);
    }
    printf("Completed joining %d threads \n", multiThreadLevel);

}

void* action(void* arg){

    ThreadContext* thCtx = (ThreadContext*)arg;

    thCtx->actionsCounter += 1;

    pthread_exit(nullptr);
}