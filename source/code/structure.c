#include "include/core.h"
#include "include/structure.h"
#include "include/errors.h"
#include <stdio.h>
#include <string.h>


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

/*
// init a list of columns with 
data_column_t *data_column_list_init(int length){
    data_column_t *columns =safe_malloc(sizeof(data_column_t),length);
    return columns;
}

void data_column_init(data_column_t *column){
    column->alias=0;
    column->object=0;
    column->ordinal=-1;
    column->type=-1;
}

// init a list of columns with 
void add_data_column(select_t *obj){
    // columns... create, copy, destroy old, replace
    // create
    data_column_t *new_columns=data_column_list_init(obj->column_length+1);
    // if existing items exist
    if(obj->columns!=0) {
        // copy
        int data_size=sizeof(data_column_t)*obj->column_length;
        memcpy(new_columns,obj->columns,data_size);
        // destroy old
        free(obj->columns);
    }

    // replace
    obj->columns=new_columns;
    //init the newest column
    data_column_init(&obj->columns[obj->column_length]);
    ++obj->column_length;
}


*/


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
    /*
    printf("\nSELECT_DEBUG\n");
    
    char *has_limit_start;
    char *has_limit_length;
    char *has_from;
    char *has_alias;
    char *has_columns;
    char *has_group;
    char *has_order;
    char *has_where;

    if(obj->alias  ==0) has_alias  ="NO"; else has_alias  ="YES";
    if(obj->columns==0) has_columns="NO"; else has_columns="YES";
    if(obj->group  ==0) has_group  ="NO"; else has_group  ="YES";
    if(obj->order  ==0) has_order  ="NO"; else has_order  ="YES";
    if(obj->where  ==0) has_where  ="NO"; else has_where  ="YES";
    if(obj->from   ==0) has_from   ="NO"; else has_from   ="YES";
    if(obj->limit_start ==0) has_limit_start ="NO"; else has_limit_start ="YES";
    if(obj->limit_length==0) has_limit_length="NO"; else has_limit_length="YES";

    printf(" -flags-----------\n");
    printf("   has_from        =%s\n"   ,has_from);
    printf("   has_alias       =%s\n"   ,has_alias);
    printf("   has_columns     =%s\n"   ,has_columns);
    printf("   has_group       =%s\n"   ,has_group);
    printf("   has_order       =%s\n"   ,has_order);
    printf("   has_where       =%s\n"   ,has_where);
    printf("   has_limit_start =%s\n"   ,has_limit_start);
    printf("   has_limit_length=%s\n"   ,has_limit_length);
    printf("   column length   =%d\n"   ,obj->column_length);
    printf("   order length    =%d\n"   ,obj->order_length);
    printf("   group length    =%d\n"   ,obj->group_length);
    printf("   where length    =%d\n"   ,obj->where_length);
    printf(" -values----------\n");
    
    printf("   alias           =%s\n"   ,obj->alias);
    printf("   distinct        =%d\n"   ,obj->distinct);
    if(obj->from){
        printf("   from qualifier  =%s\n"   ,obj->from->qualifier);
        printf("   from source     =%s\n"   ,obj->from->source);
    }
    printf("   limit_length    =%d\n"   ,obj->limit_length);
    printf("   limit_start     =%d\n"   ,obj->limit_start);
    
    
    if (obj->columns!=0){
        printf(" -data-columns----------\n");
        for(int i=0;i<obj->column_length;i++){
            printf("   alias: %s ,",obj->columns[i].alias);
            printf(" type: %s ,",token_type(obj->columns[i].type));
            if (obj->columns[i].type==TOKEN_IDENTIFIER) {
               identifier_t *ident=(identifier_t*)obj->columns[i].object;
               printf("Qualifier: %s,Source: %s",ident->qualifier,ident->source);
            }
            if (obj->columns[i].type==TOKEN_LITTERAL) {
                token_t *token=(token_t*)obj->columns[i].object;
                //printf(" sub type: %s ,");

                printf("-%s,val: %s",token_type(token->type),token->value );

            }
            printf(" ordinal: %d \n",obj->columns[i].ordinal);
        }
    }
    if (obj->where!=0){
        printf(" -where----------\n");
        for(int i=0;i<obj->where_length;i++){
            where_expr_t *where=&obj->where[i];
            printf("   -ordinal %d",where->ordinal);
            printf(" -length %d",where->length);
            printf(" -not %d \n",where->NOT);
            for(int w=0;w<where->length;w++) {
                //printf("     -expr--: ");
                token_t *token=&where->tokens[w];
                printf("     %s, %s  \n",token_type(token->type),token->value );
            }
            printf("     -comparitor %s\n",token_type(where->comparitor));
//            printf("\n");
        }
    }
    
    if (obj->group!=0){
        printf(" -group-----------\n");
        for(int i=0;i<obj->group_length;i++){
                group_column_t *group=&obj->group[i];
                printf("   Qualifier: %s,Source: %s",group->identity->qualifier,group->identity->source);
                printf(", ordinal: %d, \n",group->ordinal);
        }
    }    

    if (obj->order!=0){
        printf(" -order-----------\n");
        for(int i=0;i<obj->order_length;i++){

                order_column_t *order=&obj->order[i];
                printf("   Qualifier: %s,Source: %s",order->identity->qualifier,order->identity->source);
            
                printf(" %s, ordinal: %d\n",token_type(order->direction),order->ordinal);
        }
    }    
    */
}


