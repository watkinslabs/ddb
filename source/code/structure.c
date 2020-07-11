#include <stdio.h>
#include <string.h>
#include "../include/structure.h"
#include "../include/core.h"
#include "../include/errors.h"


select_t *select_new(){
    select_t *obj=safe_malloc(sizeof(select_t),1);
    // init data structure
    // sub elements
    obj->columns          =0;
    obj->from             =0;
    obj->join             =0;
    obj->where            =0;
    obj->group            =0;
    obj->order            =0;
    // elements
    obj->alias            =0;
    obj->distinct         =0;
    obj->limit_start      =0;
    obj->limit_length     =0;
    
    // internal
    obj->column_length    =0;
    obj->join_length      =0;
    obj->has_limit_length =0;
    obj->has_limit_start  =0;
    return obj;
}

// init a list of join's
join_t *join_list_init(int length){
    join_t *join =safe_malloc(sizeof(join_t),length);
    return join;
}

void join_init(join_t *join){
    join->alias=0;
    join->expression=0;
    join->identifier=0;
    join->ordinal=0;
    join->type=0;

}

// init a list of columns with 
void add_join(select_t *obj){
    // columns... create, copy, destroy old, replace
    // create
    join_t *new_join=join_list_init(obj->join_length+1);
    // if existing items exist
    if(obj->join!=0) {
        // copy
        int data_size=sizeof(join_t)*obj->join_length;
        memcpy(new_join,obj->join,data_size);
        // destroy old
        free(obj->join);
    }

    // replace
    obj->join=new_join;
    //init the newest column
    join_init(&obj->join[obj->join_length]);
    ++obj->join_length;
}

void set_distinct(select_t *obj){
    obj->distinct=1;
}

void debug(token_array_t *tokens){
    for(int i=0;i<tokens->position;i++){
        char *t_type=token_type(tokens->array[i].type);
        char *t_value=tokens->array[i].value;
        printf(" %15s - %15s  ",t_value,t_type);
        for(int e=tokens->array[i].depth-1;e>=0;e--){
            t_type=token_type(tokens->array[i].expr[e]);
            printf(" %15s ",t_type);
        }
        printf("\n");
    }
    return;

}


