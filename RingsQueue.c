#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>

#include <stdio.h>
#include <stdlib.h>

#include "HazardPointer.h"
#include "RingsQueue.h"

struct RingsQueueNode;
typedef struct RingsQueueNode RingsQueueNode;

struct RingsQueueNode {
    _Atomic(struct RingsQueueNode*) next;
    _Atomic(Value) push_idx;
    _Atomic(Value) pop_idx;
    Value ring_buffer[RING_SIZE];
};

RingsQueueNode* RingsQueueNode_new(Value item) {
    RingsQueueNode* node = (RingsQueueNode*)malloc(sizeof(RingsQueueNode));

    atomic_init(&node->next, NULL);
    atomic_init(&node->pop_idx, 0);
    atomic_init(&node->push_idx, 0);

    if(item == EMPTY_VALUE) {
        atomic_init(&node->push_idx, 0);
    } else {
        atomic_store(&node->push_idx, 1);
        node->ring_buffer[0] = item;
    }
    return node;
}

struct RingsQueue {
    RingsQueueNode* head;
    RingsQueueNode* tail;
    pthread_mutex_t pop_mtx;
    pthread_mutex_t push_mtx;
};

RingsQueue* RingsQueue_new(void)
{
    RingsQueue* queue = (RingsQueue*)malloc(sizeof(RingsQueue));

    RingsQueueNode* first_node = RingsQueueNode_new(EMPTY_VALUE);
    queue->head = first_node;
    queue->tail = first_node;

    pthread_mutex_init(&queue->pop_mtx, NULL);
    pthread_mutex_init(&queue->push_mtx, NULL);

    return queue;
}

void RingsQueue_delete(RingsQueue* queue)
{
    RingsQueueNode* current = queue->head;
    RingsQueueNode* next;

    while (current != NULL) {
        next = atomic_load(&current->next);
        free(current);
        current = next;
    }

    pthread_mutex_destroy(&queue->pop_mtx);
    pthread_mutex_destroy(&queue->push_mtx);
    free(queue);
}

void RingsQueue_push(RingsQueue* queue, Value item)
{
    pthread_mutex_lock(&queue->push_mtx);

    long tempFull = (atomic_load(&queue->tail->push_idx) + 1)%RING_SIZE;
    if(atomic_load(&queue->tail->pop_idx) == tempFull) {
        RingsQueueNode* new_tail = RingsQueueNode_new(item);
        atomic_store(&queue->tail->next, new_tail);
        queue->tail = new_tail;
    } else {
        queue->tail->ring_buffer[atomic_load(&queue->tail->push_idx)] = item;

        Value cyclicIncremented = (atomic_load(&queue->tail->push_idx) + 1)%RING_SIZE;
        atomic_store(&queue->tail->push_idx, cyclicIncremented);
    }

    pthread_mutex_unlock(&queue->push_mtx);
}

Value RingsQueue_pop(RingsQueue* queue)
{
    pthread_mutex_lock(&queue->pop_mtx);

    if (atomic_load(&queue->head->next) != NULL) {

        long tempEmpty = atomic_load(&queue->head->pop_idx);
        if(atomic_load(&queue->head->push_idx) == tempEmpty) {
            RingsQueueNode* old_head = queue->head;
            queue->head = atomic_load(&queue->head->next);

            pthread_mutex_unlock(&queue->pop_mtx);
            free(old_head);
            return EMPTY_VALUE;

        } else {
            Value result = queue->head->ring_buffer[atomic_load(&queue->head->pop_idx)];
            Value cyclicFixed = (atomic_load(&queue->head->pop_idx)+ 1)%RING_SIZE;
            atomic_store(&queue->head->pop_idx, cyclicFixed);

            pthread_mutex_unlock(&queue->pop_mtx);
            return result;
        }

    } else {

        long tempEmpty = atomic_load(&queue->head->pop_idx);
        if(atomic_load(&queue->head->push_idx) == tempEmpty) {
            pthread_mutex_unlock(&queue->pop_mtx);
            return EMPTY_VALUE;

        } else {
            Value result = queue->head->ring_buffer[atomic_load(&queue->head->pop_idx)];
            Value cyclicFixed = (atomic_load(&queue->head->pop_idx)+ 1)%RING_SIZE;
            atomic_store(&queue->head->pop_idx, cyclicFixed);

            pthread_mutex_unlock(&queue->pop_mtx);
            return result;
        }
    }
}

bool RingsQueue_is_empty(RingsQueue* queue)
{
    pthread_mutex_lock(&queue->pop_mtx);

    if (atomic_load(&queue->head->next) == NULL) {

        long tempEmpty = atomic_load(&queue->head->pop_idx);

        if(atomic_load(&queue->head->push_idx) == tempEmpty) {
            pthread_mutex_unlock(&queue->pop_mtx);
            return true;
        }

    }
    pthread_mutex_unlock(&queue->pop_mtx);

    return false;
}
