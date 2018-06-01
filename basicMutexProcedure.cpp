#include <iostream>
#include <string>
#include <pthread.h>

using namespace std;
#define NUM_OF_LOOPS 10000
#define THREADS_NUM 1

pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
sem_t semaphore;
pthread_cond_t aCond = PTHREAD_COND_INITIALIZER;

struct passingData {
    int sign;
    int sum;
};

//   Counts from 0 to NUM_OF_LOOPS
//   Depends on given sign (minus or plus):
void* countNum(void* incSign){

    // incSign will determine + or -
    int sign = *(int*)incSign;
    string threadSign =  (sign > 0) ? "tPlus" : "tMinus";

    pthread_mutex_lock(&mutex1);
    for (int i = 0; i< NUM_OF_LOOPS ; i++){
        cout << "From " + threadSign + " " + to_string(i)  << endl;
    }
    pthread_mutex_unlock(&mutex1);

    pthread_exit(nullptr);
}


int main()
{
    // Spawn threads:
    printf("Creating %d threads \n", THREADS_NUM);
    pthread_t threadsList[THREADS_NUM];

    for (int i = 0 ; i< THREADS_NUM; i++){
        // Set +1 for even, -1 odd
        int signOfCount = (i%2 == 0) ? 1 : -1;
        int rc = pthread_create(&threadsList[i], nullptr, countNum,
                                &signOfCount);
    }

    pthread_mutex_lock(&mutex1);
    for (int i = 0; i< NUM_OF_LOOPS ; i++){
        cout << "From main " + to_string(i)  << endl;
    }
    pthread_mutex_unlock(&mutex1);

    for (unsigned long th : threadsList)
    {
        pthread_join(th, nullptr);
    }
    printf("Completed joining %d threads \n", THREADS_NUM);

    pthread_mutex_destroy(&mutex1);
    pthread_cond_destroy(&aCond);

    return 0;
}
