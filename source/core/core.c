#include "include/errors.h"
#include <memory.h>

void * safe_malloc(int size,int qty){
    int length=size*qty;
    if(length<=0) ghost(ERR_OUT_OF_BOUNDS);
    void *ptr=(void *)calloc(1,length);
    //safe reset
    if(ptr==0) {
        ghost(ERR_MEMORY_ALLOCATION_ERR);
    }
    return ptr;
}