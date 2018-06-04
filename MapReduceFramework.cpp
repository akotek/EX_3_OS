#include <iostream>
#include <atomic>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
#include <semaphore.h>
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
    pthread_mutex_t& queueLock;
    sem_t& fillCount;
    int& shuffleEndedFlag;
    int& queueCounter;
    pthread_mutex_t& isShuffledLock;

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

    // Init semaphores - init with value 0
    // To be increase by shuffler
    pthread_mutex_t queueLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t isShuffledLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t createThreadsLock = PTHREAD_MUTEX_INITIALIZER;
    sem_t fillCount;
    sem_init(&fillCount, 0, 0);

    // Spawn threads,
    // Create a threadPool array
    // And init contexts:
    if (multiThreadLevel > 1) multiThreadLevel -=1;
  //  printf("Creating %d threads \n", multiThreadLevel);
    pthread_t threads[multiThreadLevel];
    vector<ThreadContext> contexts;
    Barrier barrier(multiThreadLevel);
    vector<IntermediateVec> allVec(multiThreadLevel); // vector of vectors
    vector<IntermediateVec> shuffledQueue;
    std::atomic<int> atomic_counter(0);
    int shuffleEnded = 0;
    int queueCounter = 0;

    for (int i = 0; i < multiThreadLevel; i++) {
        contexts.push_back(ThreadContext{i, atomic_counter, inputVec,
                                         outputVec, allVec, client, barrier,
        shuffledQueue, queueLock, fillCount, shuffleEnded, queueCounter, isShuffledLock});
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_mutex_lock(&createThreadsLock);
        pthread_create(&threads[i], nullptr, action, &contexts[i]);
        pthread_mutex_unlock(&createThreadsLock);
    }

    // Join threads:
    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_join(threads[i], nullptr);
    }
  //  printf("Completed joining %d threads \n", multiThreadLevel);

    // Destroy semaphore && mutex:
    pthread_mutex_destroy(&queueLock);
    pthread_mutex_destroy(&createThreadsLock);
    sem_destroy(&fillCount);
   // printf("Completed destroying Mutex and Semaphore \n");
}

void emit2 (K2* key, V2* value, void* context){

    ThreadContext* threadContext = (ThreadContext*)context;

    IntermediateVec& vec = threadContext->allVec[threadContext->threadId];
    IntermediatePair pair = {key, value};

    // Vec copies pair by value
    vec.push_back(pair);
}

void emit3 (K3* key, V3* value, void* context)
{
    ThreadContext* threadContext = (ThreadContext*)context;

    OutputVec& vec = threadContext->outputVec;
    OutputPair pair = {key, value};

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

void shuffleHandler(ThreadContext *threadContext)
{

    // Find kMax value in all interMid pairs,
    // Collect from all vectors into a new one
    // Push at the end to new queue:
    bool isEmpty = false;

    vector<IntermediateVec>& allVec = threadContext->allVec;
    while (!isEmpty)
    {
        if(all_of(allVec.begin(), allVec.end(), [](IntermediateVec& vec){
            return vec.empty(); }))
        {
            break;
        }

        IntermediateVec tempMaxVec;
        K2& k2max = findK2max(allVec);

        for(IntermediateVec& intermediateVec : allVec)
        {
            if(intermediateVec.empty())
            {

                allVec.erase(std::remove(allVec.begin(), allVec.end(), intermediateVec),
                             allVec.end());
            }
            while (!intermediateVec.empty() &&
                   *(intermediateVec.back()).first == k2max)
            {
                tempMaxVec.push_back(intermediateVec[intermediateVec.size()-1]);
                intermediateVec.pop_back();

//                if(intermediateVec.empty())
//                {
//                    allVec.erase(std::remove(allVec.begin(), allVec.end(), intermediateVec),
//                                 allVec.end());
//                }
            }


        }

        // Lock
        pthread_mutex_lock(&(threadContext->queueLock));
        threadContext->queue.push_back(tempMaxVec);
        tempMaxVec.clear();
        threadContext->queueCounter++;
        // Unlock
        // Semaphore wake up threads:
        sem_post(&threadContext->fillCount);
        pthread_mutex_unlock(&threadContext->queueLock);

    }
    // Shuffle ended: make True
    pthread_mutex_lock(&threadContext->queueLock);
    threadContext->shuffleEndedFlag = 1;
    pthread_mutex_unlock(&threadContext->queueLock);
}

void* action(void* arg){

    ThreadContext* threadContext = (ThreadContext*)arg;

    // Increment val
    int oldVal = (threadContext->actionsCounter)++;

    // Get pair of oldVal and map it
    const MapReduceClient& client = threadContext->client;
    const int inputSize = (int)threadContext->inputVec.size();

    while (oldVal < inputSize){
        // Map:
        const InputPair& pair = threadContext->inputVec[oldVal];
        client.map(pair.first, pair.second, threadContext);
        oldVal = (threadContext->actionsCounter)++;
    }
    // Sort
    IntermediateVec& intermediateVec = threadContext->allVec[threadContext->threadId];
    std::sort(intermediateVec.begin(), intermediateVec.end(), InterMidGreater);

    // Launch barrier:
    threadContext->barrier.barrier();

    // Shuffle by thread 0:
    if (threadContext->threadId == 0)
    {
        shuffleHandler(threadContext);
    }

    // Reduce:
    vector<IntermediateVec>& queue = threadContext->queue;
//    IntermediateVec queuePiece;

    // while True:
//    while (true)
//    {
//
//        sem_wait(&threadContext->fillCount);
//        // lock shuffle queue before access
//        pthread_mutex_lock(&threadContext->queueLock);
//        if (threadContext->queueCounter == 0 &&
//                threadContext->shuffleEndedFlag){
//            sem_post(&threadContext->fillCount);
//            pthread_mutex_unlock(&threadContext->queueLock);
//            break;
//        }
//        else if (threadContext->queueCounter > 0)
//        {
//            // Lock
////            queuePiece = queue[queue.size()-1];
//            client.reduce(&queue.back(), threadContext);
//            queue.pop_back();
//            threadContext->queueCounter--;
//            // Unlock
//        }
//        pthread_mutex_unlock(&threadContext->queueLock);
//        sem_post(&threadContext->fillCount);
//
//    }


//    pthread_exit(nullptr);
}