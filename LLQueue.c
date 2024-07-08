#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "HazardPointer.h"
#include "LLQueue.h"

struct LLNode;
typedef struct LLNode LLNode;
typedef _Atomic(LLNode*) AtomicLLNodePtr;

struct LLNode {
    AtomicLLNodePtr next;
    _Atomic(Value) item;
};

LLNode* LLNode_new(Value item)
{
    LLNode* node = (LLNode*)malloc(sizeof(LLNode));

    atomic_init(&node->item, item);
    atomic_init(&node->next, NULL);

    return node;
}

struct LLQueue {
    AtomicLLNodePtr head;
    AtomicLLNodePtr tail;
    HazardPointer hp;
};

LLQueue* LLQueue_new(void)
{
    LLQueue* queue = (LLQueue*)malloc(sizeof(LLQueue));

    LLNode* first_node = LLNode_new(EMPTY_VALUE);
    atomic_init(&queue->head, first_node);
    atomic_init(&queue->tail, first_node);

    HazardPointer_initialize(&queue->hp);

    return queue;
}

void LLQueue_delete(LLQueue* queue)
{
    HazardPointer_finalize(&queue->hp);

    LLNode* current = atomic_load(&queue->head);
    LLNode* next;

    while (current != NULL) {
        next = atomic_load(&current->next);
        free(current);
        current = next;
    }

    free(queue);
}

void LLQueue_push(LLQueue* queue, Value item)
{
    LLNode* tail;
    _Atomic(void*) tailCandidate;

    LLNode* newHolder = LLNode_new(item);
    LLNode* nextHolder;
    bool indexHolderInitialized = false;

    do {

        if(indexHolderInitialized) {
            atomic_compare_exchange_strong(&queue->tail, &tail, nextHolder);
        }

        do {
            tailCandidate = atomic_load(&queue->tail);
            tail = HazardPointer_protect(&queue->hp, &tailCandidate);
        } while (tail != atomic_load(&queue->tail));

        nextHolder = NULL;
        indexHolderInitialized = true;

    } while(!atomic_compare_exchange_strong(&tail->next, &nextHolder, newHolder));

    atomic_compare_exchange_strong(&queue->tail, &tail, newHolder);

    HazardPointer_clear(&queue->hp);
}

Value LLQueue_pop(LLQueue* queue)
{
    LLNode* head;
    _Atomic(void*) headCandidate;

    Value valueHolder;
    LLNode* nextHolder;
    bool emptyCheck = false;

    do {

        do {

            if (emptyCheck) {

                if (nextHolder == NULL) {

                    HazardPointer_clear(&queue->hp);
                    return EMPTY_VALUE;

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
                headCandidate = atomic_load(&queue->head);
                head = HazardPointer_protect(&queue->hp, &headCandidate);
            } while (head != atomic_load(&queue->head));

            valueHolder = atomic_load(&head->item);
            nextHolder = atomic_load(&head->next);

            emptyCheck = true;
        } while (valueHolder == EMPTY_VALUE);
        emptyCheck = false;

    } while (!atomic_compare_exchange_strong(&head->item, &valueHolder, EMPTY_VALUE));

    if (nextHolder == NULL) {

        HazardPointer_clear(&queue->hp);
        return valueHolder;

    } else {

        if (atomic_compare_exchange_strong(&queue->head, &head, nextHolder)) {
            HazardPointer_clear(&queue->hp);
            HazardPointer_retire(&queue->hp, head);
        } else {
            HazardPointer_clear(&queue->hp);
        }
    }

    return valueHolder;
}

bool LLQueue_is_empty(LLQueue* queue)
{
    LLNode* head;
    _Atomic(void*) headCandidate;

    Value valueHolder;
    LLNode* nextHolder;

    do {
        headCandidate = atomic_load(&queue->head);
        head = HazardPointer_protect(&queue->hp, &headCandidate);
    } while (head != atomic_load(&queue->head));

    valueHolder = atomic_load(&head->item);
    nextHolder = atomic_load(&head->next);

    if(valueHolder == EMPTY_VALUE && nextHolder == NULL) {

        HazardPointer_clear(&queue->hp);
        return true;
    }

    HazardPointer_clear(&queue->hp);

    return false;
}
