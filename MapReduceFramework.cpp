#include <iostream>
#include <atomic>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
using namespace std;

struct ThreadContext{
    int threadId;
    std::atomic<int>& actionsCounter;
    const InputVec& inputVec;
    OutputVec& outputVec;
    vector<IntermediateVec>& allVec;
    const MapReduceClient& client;
    Barrier& barrier;
    vector<IntermediateVec>& queue;

};

struct {
    bool operator()(const IntermediatePair& a, const IntermediatePair& b) const
    {
        return *a.first < *b.first;
    }

} InterMidGreater;

// equal operator for the shuffle section
bool operator==(const IntermediatePair& a, const IntermediatePair& b)
{
    return !(*a.first < *b.first) && !(*b.first < *a.first);
}

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
    multiThreadLevel -=1; //TODO find out about mainThread
    printf("Creating %d threads \n", multiThreadLevel);
    pthread_t threads[multiThreadLevel];
    vector<ThreadContext> contexts;
    Barrier barrier(multiThreadLevel);
    vector<IntermediateVec> allVec(multiThreadLevel); // vector of vectors
    vector<IntermediateVec> shuffledQueue(multiThreadLevel);
    std::atomic<int> atomic_counter(0);

    for (int i = 0; i < multiThreadLevel; i++) {
        contexts.push_back(ThreadContext{i, atomic_counter, inputVec,
                                         outputVec, allVec, client, barrier,
        shuffledQueue});
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_create(&threads[i], nullptr, action, &contexts[i]);
    }

    // Join threads:
    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_join(threads[i], nullptr);
    }
    printf("Completed joining %d threads \n", multiThreadLevel);

    // Printing data
    for(IntermediateVec &v : allVec){
        cout << v.size() << endl;
    }
}


void emit2 (K2* key, V2* value, void* context){

    ThreadContext* thCtx = (ThreadContext*)context;

    IntermediateVec& vec = thCtx->allVec[thCtx->threadId];
    IntermediatePair pair = {key, value};

    // Vec copies pair by value
    vec.push_back(pair);
}

void findK2max(vector<IntermediateVec>& allVec)
{
    //
    K2* k2max = allVec[0][allVec[0].size()-1].first;

    for(IntermediateVec& intermediateVec : allVec)
    {
        K2* tempKey = intermediateVec[intermediateVec.size()-1].first;
        if (k2max < tempKey)
        {
            k2max = tempKey;
        }

    }
    cout << k2max << endl;
//    return k2max;
}

void* action(void* arg){

    ThreadContext* threadContext = (ThreadContext*)arg;

    // Increment val
    int oldVal = (threadContext->actionsCounter)++;

    // Get pair of oldVal and map it
    const MapReduceClient& client = threadContext->client;
    const int inputSize = (int)threadContext->inputVec.size();

    while (oldVal < inputSize){
        // Map
        const InputPair& pair = threadContext->inputVec[oldVal];
        client.map(pair.first, pair.second, threadContext);
        oldVal = (threadContext->actionsCounter)++;
    }
    // Sort
    IntermediateVec& intermediateVec = threadContext->allVec[threadContext->threadId];
    std::sort(intermediateVec.begin(), intermediateVec.end(), InterMidGreater);


    // Launch barrier
    threadContext->barrier.barrier();

    // Shuffle by thread 0:
    if (threadContext->threadId == 0){
        vector<IntermediateVec>& allVec = threadContext->allVec;
        findK2max(allVec);
    }




    pthread_exit(nullptr);
}