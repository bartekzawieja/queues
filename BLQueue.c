#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "BLQueue.h"
#include "HazardPointer.h"

struct BLNode;
typedef struct BLNode BLNode;
typedef _Atomic(BLNode*) AtomicBLNodePtr;

struct BLNode {
    AtomicBLNodePtr next;
    _Atomic(Value) push_idx;
    _Atomic(Value) pop_idx;
    _Atomic(Value) node_buffer[BUFFER_SIZE];
};

BLNode* BLNode_new(Value item) {
    BLNode* node = (BLNode*)malloc(sizeof(BLNode));

    if(item == EMPTY_VALUE) {
        atomic_init(&node->next, NULL);
        atomic_init(&node->pop_idx, 0);
        atomic_init(&node->push_idx, 0);

        for(int i = 0; i < BUFFER_SIZE; ++i) {
            atomic_init(&node->node_buffer[i], EMPTY_VALUE);
        }
    } else {
        atomic_init(&node->next, NULL);
        atomic_init(&node->pop_idx, 0);
        atomic_init(&node->push_idx, 1);

        atomic_init(&node->node_buffer[0], item);
        for(int i = 1; i < BUFFER_SIZE; ++i) {
            atomic_init(&node->node_buffer[i], EMPTY_VALUE);
        }
    }
    return node;
}

struct BLQueue {
    AtomicBLNodePtr head;
    AtomicBLNodePtr tail;
    HazardPointer hp;
};

BLQueue* BLQueue_new(void)
{
    BLQueue* queue = (BLQueue*)malloc(sizeof(BLQueue));

    BLNode* first_node = BLNode_new(EMPTY_VALUE);
    atomic_init(&queue->head, first_node);
    atomic_init(&queue->tail, first_node);

    HazardPointer_initialize(&queue->hp);

    return queue;
}

void BLQueue_delete(BLQueue* queue)
{
    HazardPointer_finalize(&queue->hp);

    BLNode* current = atomic_load(&queue->head);
    BLNode* next;

    while (current != NULL) {
        next = atomic_load(&current->next);
        free(current);
        current = next;
    }

    free(queue);
}

void BLQueue_push(BLQueue* queue, Value item)
{
    BLNode* tail;
    _Atomic(void*) tailCandidate;

    Value valueHolder;
    Value indexHolder;
    BLNode* nextHolder;
    BLNode* newHolder;
    bool bufferSizeCheck = false;

    do {
        do {
            if (bufferSizeCheck) {
                if (nextHolder != NULL) {
                    atomic_compare_exchange_strong(&queue->tail, &tail, nextHolder);
                } else {

                    newHolder = BLNode_new(item);
                    if(atomic_compare_exchange_strong(&tail->next, &nextHolder, newHolder)) {
                        atomic_compare_exchange_strong(&queue->tail, &tail, newHolder);
                    } else {
                        free(newHolder);
                    }
                }
            }

            do {
                do {
                    tailCandidate = atomic_load(&queue->tail);
                    tail = HazardPointer_protect(&queue->hp, &tailCandidate);
                } while (tail != atomic_load(&queue->tail));

                valueHolder = EMPTY_VALUE;
                indexHolder = atomic_load(&tail->push_idx);
                nextHolder = atomic_load(&tail->next);

            } while (!atomic_compare_exchange_strong(&tail->push_idx, &indexHolder, indexHolder + 1));

            bufferSizeCheck = true;
        } while (indexHolder + 1 >= BUFFER_SIZE);
        bufferSizeCheck = false;

    } while (!atomic_compare_exchange_strong(&tail->node_buffer[indexHolder+1], &valueHolder, item));
    HazardPointer_clear(&queue->hp);
}

Value BLQueue_pop(BLQueue* queue)
{
    BLNode* head = NULL;
    _Atomic(void*) headCandidate = NULL;

    Value valueHolder;
    Value indexHolder;
    BLNode* nextHolder;
    bool emptyCheck = false;

    do {
        do {
            if (emptyCheck) {

                if (nextHolder == NULL) {
                    HazardPointer_clear(&queue->hp);
                    return EMPTY_VALUE; //
                } else {

                    if (atomic_compare_exchange_strong(&queue->head, &head, nextHolder)) {
                        HazardPointer_clear(&queue->hp);
                        HazardPointer_retire(&queue->hp, head);
                    } else {
                        HazardPointer_clear(&queue->hp);
                    }
                }
            }

            do {

                do {
                    headCandidate = atomic_load(&queue->head);
                    head = HazardPointer_protect(&queue->hp, &headCandidate);
                } while (head != atomic_load(&queue->head));

                valueHolder = EMPTY_VALUE;
                indexHolder = atomic_load(&head->pop_idx);
                nextHolder = atomic_load(&head->next);

            } while (!atomic_compare_exchange_strong(&head->pop_idx, &indexHolder, indexHolder + 1));

            emptyCheck = true;
        } while (indexHolder + 1 >= BUFFER_SIZE);
        emptyCheck = false;

    } while (atomic_compare_exchange_strong(&head->node_buffer[indexHolder+1], &valueHolder, TAKEN_VALUE));
    HazardPointer_clear(&queue->hp);

    return valueHolder;
}

bool BLQueue_is_empty(BLQueue* queue)
{
    BLNode* head;
    _Atomic(void*) headCandidate;

    BLNode* nextHolder;
    Value indexHolder;

    do {
        headCandidate = atomic_load(&queue->head);
        head = HazardPointer_protect(&queue->hp, &headCandidate);
    } while (head != atomic_load(&queue->head));

    indexHolder = atomic_load(&head->pop_idx);
    nextHolder = atomic_load(&head->next);

    if(indexHolder + 1 >= BUFFER_SIZE && nextHolder == NULL) {
        HazardPointer_clear(&queue->hp);
        return EMPTY_VALUE;
    }

    HazardPointer_clear(&queue->hp);

    return false;
}
