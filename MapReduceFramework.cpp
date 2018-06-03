#include <iostream>
#include <atomic>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
#include <semaphore.h>

using namespace std;

pthread_mutex_t queueLock = PTHREAD_MUTEX_INITIALIZER;
sem_t fillCount;


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

// equal operator for the shuffleHandler section
bool operator==(const IntermediatePair& a, const IntermediatePair& b)
{
    return !(*a.first < *b.first) && !(*b.first < *a.first);
}

bool operator==(const K2& a, const K2& b)
{
    return !(a < b) && !(b < a);
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
    vector<IntermediateVec> shuffledQueue;
    std::atomic<int> atomic_counter(0);

    // Init semaphores - init with value 0
    // To be increase by shuffler
    sem_init(&fillCount, 0, 0);


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
//    for(IntermediateVec &v : allVec){
//        cout << v.size() << endl;
//    }
}


void emit2 (K2* key, V2* value, void* context){

    ThreadContext* thCtx = (ThreadContext*)context;

    IntermediateVec& vec = thCtx->allVec[thCtx->threadId];
    IntermediatePair pair = {key, value};

    // Vec copies pair by value
    vec.push_back(pair);
}

K2& findK2max(vector<IntermediateVec>& allVec)
{
    K2* k2max = allVec[0][allVec[0].size()-1].first;


    for(IntermediateVec& intermediateVec : allVec)
    {
        if (!intermediateVec.empty())
        {
            K2 *tempKey = intermediateVec[intermediateVec.size() - 1].first;
            if (k2max < tempKey)
            {
                k2max = tempKey;
            }
        }
    }
    return *k2max;
}

void emit3 (K3* key, V3* value, void* context)
{

}




/**
 *
 * @param threadContext
 */
void shuffleHandler(ThreadContext *threadContext)
{

    vector<IntermediateVec>& allVec = threadContext->allVec;

    while (!allVec.empty())
    {

        IntermediateVec tempMaxVec;
        K2& k2max = findK2max(allVec);

        for(IntermediateVec& intermediateVec : allVec)
        {
            while (!intermediateVec.empty() &&
                   *(intermediateVec[intermediateVec.size()-1]).first == k2max)
            {
                tempMaxVec.push_back(intermediateVec[intermediateVec.size()-1]);
                intermediateVec.pop_back();

                if(intermediateVec.empty())
                {
                    allVec.erase(std::remove(allVec.begin(), allVec.end(), intermediateVec),
                                 allVec.end()); // remove
                }
            }
        }
        // mutex_lock
        pthread_mutex_lock(&queueLock);
        threadContext->queue.push_back(tempMaxVec);
        tempMaxVec.clear();
        // mutex_unlock
        pthread_mutex_unlock(&queueLock);

        // semaphore wake up threads
        sem_post(&fillCount);
    }

    cout << "done" << endl;
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
    if (threadContext->threadId == 0)
    {
        shuffleHandler(threadContext);
    }


    // Reduce
    vector<IntermediateVec>& queue = threadContext->queue;

    if(!queue.empty())
    {
        sem_wait(&fillCount);

        // mutex_lock before critical section
        pthread_mutex_lock(&queueLock);
        IntermediateVec& queuePiece = queue[queue.size()-1];
        queue.pop_back();
        // mutex_unlock
        pthread_mutex_unlock(&queueLock);

        client.reduce(&queuePiece, threadContext);
    }


    pthread_exit(nullptr);
}