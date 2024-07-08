#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <threads.h>

#include "HazardPointer.h"

thread_local int _thread_id = -1;
int _num_threads = -1;

void HazardPointer_register(int thread_id, int num_threads)
{
    _thread_id = thread_id;
    //_num_threads = num_threads;

    if(thread_id == 0) {
        _num_threads = num_threads;
    }
}

void HazardPointer_initialize(HazardPointer* hp)
{
    int i, j;

    for (i = 0; i < MAX_THREADS; ++i) {
        atomic_init(&hp->pointer[i], NULL);

        int* metadata = (int*)malloc(sizeof(int));

        hp->retired[i][MAX_THREADS+1] = (void*)metadata;

        *(int*)(hp->retired[i][MAX_THREADS+1]) = 0;

        for (j = 0; j < MAX_THREADS+1; ++j) {
            hp->retired[i][j] = NULL;
        }
    }
}

void HazardPointer_finalize(HazardPointer* hp)
{
    for (int i = 0; i < MAX_THREADS; ++i) {

        atomic_store(&hp->pointer[i], NULL);

        for (int j = 0; j <= MAX_THREADS+1; ++j) {
            if(hp->retired[i][j] != NULL) {
                free(hp->retired[i][j]);
                hp->retired[i][j] = NULL;
            }
        }
    }
}

void* HazardPointer_protect(HazardPointer* hp, const _Atomic(void*)* atom)
{
    void* protectedAddress = atomic_load(atom);
    atomic_store(&hp->pointer[_thread_id], protectedAddress);

    void* myHazardPointer = atomic_load(&hp->pointer[_thread_id]);
    if(atomic_load(atom) == myHazardPointer) {
        return (void*) myHazardPointer;
    } else {
        atomic_store(&hp->pointer[_thread_id], NULL);
        return (void*) NULL;
    }
}

void HazardPointer_clear(HazardPointer* hp)
 {
     atomic_store(&hp->pointer[_thread_id], NULL);
 }

void HazardPointer_retire(HazardPointer* hp, void* ptr)
{
    int i, j;

    void* retired_address;
    int* count = (int*)(hp->retired[_thread_id][MAX_THREADS+1]);

    for(i = 0; i < MAX_THREADS+1; ++i) {
        if(hp->retired[_thread_id][i] == NULL) {
            hp->retired[_thread_id][i] = ptr;
            (*count)++;
            break;
        }
    }

    if( (*count) > RETIRED_THRESHOLD) {

        for (i = 0; i < MAX_THREADS+1; ++i) {

            retired_address = hp->retired[_thread_id][i];

            if(retired_address == NULL) continue;

            for (j = 0; j < _num_threads; ++j) {
                if (atomic_load(&hp->pointer[j]) == retired_address) {
                    break;
                }
            }

            if (j == _num_threads) {
                hp->retired[_thread_id][i] = NULL;
                free(retired_address);
                (*count)--;
            }
        }

    }

}
