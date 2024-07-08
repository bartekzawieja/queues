#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>

#include "SimpleQueue.h"

struct SimpleQueueNode;
typedef struct SimpleQueueNode SimpleQueueNode;

struct SimpleQueueNode {
    _Atomic(SimpleQueueNode*) next;
    Value item;
};

SimpleQueueNode* SimpleQueueNode_new(Value item)
{
    SimpleQueueNode* node = (SimpleQueueNode*)malloc(sizeof(SimpleQueueNode));

    node->item = item;
    atomic_init(&node->next, NULL);

    return node;
}

struct SimpleQueue {
    SimpleQueueNode* head;
    SimpleQueueNode* tail;
    pthread_mutex_t head_mtx;
    pthread_mutex_t tail_mtx;
};

SimpleQueue* SimpleQueue_new(void)
{
    SimpleQueue* queue = (SimpleQueue*)malloc(sizeof(SimpleQueue));

    SimpleQueueNode* dummy_head = SimpleQueueNode_new(EMPTY_VALUE);
    queue->tail = dummy_head;
    queue->head = dummy_head;

    pthread_mutex_init(&queue->head_mtx, NULL);
    pthread_mutex_init(&queue->tail_mtx, NULL);

    return queue;
}

void SimpleQueue_delete(SimpleQueue* queue)
{
    SimpleQueueNode* current = queue->head;
    SimpleQueueNode* next;

    while (current != NULL) {
        next = atomic_load(&current->next);
        free(current);
        current = next;
    }

    pthread_mutex_destroy(&queue->head_mtx);
    pthread_mutex_destroy(&queue->tail_mtx);

    free(queue);
}

void SimpleQueue_push(SimpleQueue* queue, Value item)
{

    SimpleQueueNode* new_node = SimpleQueueNode_new(item);

    pthread_mutex_lock(&queue->tail_mtx);

    atomic_store(&queue->tail->next, new_node);
    queue->tail = new_node;

    pthread_mutex_unlock(&queue->tail_mtx);
}

Value SimpleQueue_pop(SimpleQueue* queue)
{
    pthread_mutex_lock(&queue->head_mtx);

    SimpleQueueNode* firstNode = atomic_load(&queue->head->next);

    if(firstNode != NULL) {

        Value result = firstNode->item;
        firstNode->item = EMPTY_VALUE;

        SimpleQueueNode* dummy_head = queue->head;
        queue->head = firstNode;

        pthread_mutex_unlock(&queue->head_mtx);

        free(dummy_head);

        return result;
    }

    pthread_mutex_unlock(&queue->head_mtx);
    return EMPTY_VALUE;
}

bool SimpleQueue_is_empty(SimpleQueue* queue)
{
    pthread_mutex_lock(&queue->head_mtx);
    if (atomic_load(&queue->head->next) == NULL) {
        pthread_mutex_unlock(&queue->head_mtx);
        return true;
    }
    pthread_mutex_unlock(&queue->head_mtx);

    return false;
}
