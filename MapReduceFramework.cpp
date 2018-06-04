#include <iostream>
#include <atomic>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
#include <semaphore.h>
#include <assert.h>

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
    IntermediateVec& tempMaxVec;
    IntermediatePair* tempMaxPair;
    int& counter;
    K2* k2max;



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
    bool c = !(a < b) && !(b < a);
    return c;
}

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
    printf("Test succeeded \nChecked %d vectors for their equality\n", counter);
}


// --------------------------------
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
    IntermediateVec tempMaxVec;
    IntermediatePair tempMaxPair;
    int counter = 0;
    K2* k2max;


    for (int i = 0; i < multiThreadLevel; i++) {
        contexts.push_back(ThreadContext{i, atomic_counter, inputVec,
                                         outputVec, allVec, client, barrier,
        shuffledQueue, queueLock, fillCount, shuffleEnded, queueCounter, tempMaxVec,
                                         &tempMaxPair, counter, k2max});
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

void findK2max(ThreadContext *threadContext)
{
    bool isK2maxInitialized = false;
    for (IntermediateVec& vec : threadContext->allVec)
    {
        if(vec.empty()) continue;

        if (!isK2maxInitialized)
        {
            // first entry initialization
            threadContext->k2max = vec.back().first;
            isK2maxInitialized = true;
        }
        else if(threadContext->k2max < &(*vec.back().first))
        {
            threadContext->k2max = vec.back().first;
        }
    }
}



void shuffleHandler(ThreadContext *threadContext)
{
    // checks if all sub-vectors are empty
    while (!(all_of(threadContext->allVec.begin(),
                         threadContext->allVec.end(),
                         isEmpty)))
    {
        findK2max(threadContext);

        for (IntermediateVec& vec : threadContext->allVec)
        {
            if(vec.empty()) continue;


            while ((*threadContext->k2max) == (*vec.back().first))
            {
                threadContext->tempMaxPair = &vec.back();
                threadContext->tempMaxVec.push_back(
                        *threadContext->tempMaxPair);
                vec.pop_back();
                if (vec.empty())
                {
                    break;
                }
            }
        }
        pthread_mutex_lock(&threadContext->queueLock);
        threadContext->queue.push_back(threadContext->tempMaxVec);
        sem_post(&threadContext->fillCount);
        threadContext->counter++;
        pthread_mutex_unlock(&threadContext->queueLock);
        threadContext->tempMaxVec.clear();
    }
    pthread_mutex_lock(&threadContext->queueLock);
//    *tc->shuffle_flag = 0;
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
        shuffleTest(threadContext->queue);
    }



    // Reduce:
//    vector<IntermediateVec>& queue = threadContext->queue;
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

    pthread_exit(nullptr);
}