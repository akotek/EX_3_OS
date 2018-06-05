#include <iostream>
#include <atomic>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
#include <semaphore.h>
using namespace std;


// Thread context
// ------------------
struct ThreadContext{
    int threadId;
    std::atomic<int>& actionsCounter;
    const InputVec& inputVec;
    OutputVec& outputVec;
    const MapReduceClient& client;
    Barrier& barrier;
    vector<IntermediateVec>& allVec;
    vector<IntermediateVec>& queue;
    IntermediateVec& tempMaxVec; // Not sure it should be here
    IntermediatePair* tempMaxPair; // Not sure it should be here
    int& shuffleEndedFlag; // Boolean
    K2* k2max;
    sem_t& fillCount;
    pthread_mutex_t& queueLock;
    pthread_mutex_t& reduceLock;
};
// ------------------

// Operators for sorting (== op)
// ------------------
struct {
    bool operator()(const IntermediatePair& a, const IntermediatePair& b) const
    {
        return *a.first < *b.first;
    }

} InterMidGreater;

bool operator==(const IntermediatePair& a, const IntermediatePair& b)
{
    return !(*a.first < *b.first) && !(*b.first < *a.first);
}

bool operator==(const K2& a, const K2& b)
{
    bool c = !(a < b) && !(b < a);
    return c;
}
// ------------------

// Lambda
static const auto isEmpty = [](IntermediateVec& vec) { return vec.empty();};


// func decelerations:
// ---------------
void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);
void* action(void* arg);
// ---------------

// TESTING SECTION :: REMOVE AFTER
// --------------------------------
void shuffleTest(vector<IntermediateVec>& queue){

    int counter = 0;
    for (auto &vec : queue)
    {
        counter++;
        K2& key = *(vec[0]).first;
        for (IntermediatePair& pair1: vec){
            if (!(*(pair1.first) == key)){
                fprintf(stderr, "Shuffle failed on vec num %d", counter);
                exit(1);
            }
        }
    }
    printf("Test succeeded \nChecked %d vectors for their equality\n",
            counter);
}
// --------------------------------
void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel){

    // Init semaphores - init with value 0
    // To be increased by shuffler
    pthread_mutex_t queueLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t reduceLock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t createThreadsLock = PTHREAD_MUTEX_INITIALIZER;
    sem_t fillCount;
    sem_init(&fillCount, 0, 0);

    // Spawn threads,
    // Create a threadPool array
    // And init contexts:
    if (multiThreadLevel > 1) multiThreadLevel -=1;
   // printf("Creating %d threads \n", multiThreadLevel);
    pthread_t threads[multiThreadLevel];
    vector<ThreadContext> contexts;
    Barrier barrier(multiThreadLevel);
    vector<IntermediateVec> allVec(multiThreadLevel); // vector of vectors
    vector<IntermediateVec> shuffledQueue;
    std::atomic<int> atomic_counter(0);
    int shuffleEnded = 0;
    IntermediateVec tempMaxVec;
    IntermediatePair tempMaxPair;
    K2* k2max;

    for (int i = 0; i < multiThreadLevel; i++) {
        contexts.push_back(ThreadContext{i, atomic_counter, inputVec,
                                         outputVec, client, barrier, allVec,
        shuffledQueue, tempMaxVec, &tempMaxPair, shuffleEnded, k2max, fillCount
        , queueLock, reduceLock});
    }

    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_create(&threads[i], nullptr, action, &contexts[i]);
    }

    // Join threads:
    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_join(threads[i], nullptr);
    }
  //  printf("Completed joining %d threads \n", multiThreadLevel);


    // Destroy semaphore && mutex:
    pthread_mutex_destroy(&queueLock);
    pthread_mutex_destroy(&createThreadsLock);
    pthread_mutex_destroy(&reduceLock);
    sem_destroy(&fillCount);
 //   printf("Completed destroying Mutex and Semaphore \n");
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

    // Lock
    pthread_mutex_lock(&threadContext->reduceLock);
    OutputVec& vec = threadContext->outputVec;
    OutputPair pair = {key, value};

    vec.push_back(pair); // Copies by val
    // Unlock
    pthread_mutex_unlock(&threadContext->reduceLock);
}

void findK2max(ThreadContext *threadContext)
{
    bool isK2maxInitialized = false;
    for (IntermediateVec& vec : threadContext->allVec)
    {
        if(vec.empty()) continue;

        if (!isK2maxInitialized)
        {
            // First entry initialization
            threadContext->k2max = vec.back().first;
            isK2maxInitialized = true;
        }
        else if(*threadContext->k2max < (*vec.back().first)) // Uses inner-op
        {
            threadContext->k2max = vec.back().first;
        }
    }
}

void shuffleHandler(ThreadContext *threadContext)
{
    // Lambda: checks if all sub-Vecs are empty:
    while (!(all_of(threadContext->allVec.begin(), threadContext->allVec.end(),
                         isEmpty)))
    {

        findK2max(threadContext);
        for (IntermediateVec& vecToShuffle : threadContext->allVec)
        {
            if(vecToShuffle.empty()) continue;
            while ((*threadContext->k2max) == (*vecToShuffle.back().first))
            {
                // Gather all same K's to one vec,
                // Push at the end
                threadContext->tempMaxPair = &vecToShuffle.back();
                threadContext->tempMaxVec.push_back(*threadContext->tempMaxPair);
                vecToShuffle.pop_back();
                if (vecToShuffle.empty())
                {
                    break;
                }
            }
        }
        // Lock
        pthread_mutex_lock(&threadContext->queueLock);
        threadContext->queue.push_back(threadContext->tempMaxVec);
        sem_post(&threadContext->fillCount);
        threadContext->tempMaxVec.clear(); //TODO check if this belongs here??
        // Unlock
        pthread_mutex_unlock(&threadContext->queueLock);
    }

    threadContext->shuffleEndedFlag = 1;
}

void* action(void* arg){

    ThreadContext* threadContext = (ThreadContext*)arg;

    // Increment val
    int oldVal = (threadContext->actionsCounter)++;

    // Map:
    // Get pair of oldVal and map it
    const MapReduceClient& client = threadContext->client;
    const int inputSize = (int)threadContext->inputVec.size();

    while (oldVal < inputSize){
        const InputPair& pair = threadContext->inputVec[oldVal];
        client.map(pair.first, pair.second, threadContext);
        oldVal = (threadContext->actionsCounter)++;
    }
    // Sort:
    IntermediateVec& intermediateVec = threadContext->allVec[threadContext->threadId];
    std::sort(intermediateVec.begin(), intermediateVec.end(), InterMidGreater);

    // Launch barrier:
    threadContext->barrier.barrier();

    // Shuffle by thread 0:
    if (threadContext->threadId == 0)
    {
        shuffleHandler(threadContext);
        //shuffleTest(threadContext->queue);
    }

    // Reduce:
    vector<IntermediateVec>& queue = threadContext->queue;
    while (true)
    {
        sem_wait(&threadContext->fillCount);
        if(!queue.empty())
        {
            // Lock
            pthread_mutex_lock(&threadContext->queueLock);
            client.reduce(&queue.back(), threadContext);
            queue.pop_back();
            // Unlock
            pthread_mutex_unlock(&threadContext->queueLock);
        }
        // Shuffle ended, stop reducing:
        if (queue.empty() && threadContext->shuffleEndedFlag)
        {
            pthread_mutex_unlock(&threadContext->queueLock);
            sem_post(&threadContext->fillCount);
            break;
        }
    }
    pthread_exit(nullptr);
}