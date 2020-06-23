#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"


/* Function: duplicate_token
 * -----------------------
 * returns a copy of a token_t from an token_array_t
 * 
 * returns: token_t if the token and index are valid
 *          returns zero (NULL) otherwise
 */
token_t * duplicate_token(token_t *src){
    if(src==0) return 0;
    token_t *dst=safe_malloc(sizeof(token_t),1); 
    dst->depth  =src->depth;
    dst->type   =src->type;
    dst->value  =string_duplicate(src->value);
    for(int i=0;i<TOKEN_MAX_DEPTH;i++) dst->expr[i]=src->expr[i];
    return dst;
}

identifier_t *duplicate_identifier(identifier_t *ident){
    identifier_t *new_ident=0;
    if(ident){
        new_ident=safe_malloc(sizeof(identifier_t),1);
        new_ident->qualifier=string_duplicate(ident->qualifier);
        new_ident->source=string_duplicate(ident->source);
    }
    return new_ident;
}

data_column_t * duplicate_columns(data_column_t *columns){
    data_column_t *new_columns=0;
    data_column_t *tmp_ptr=columns;

    if(columns) {
        while(tmp_ptr) {
            data_column_t *new_column=safe_malloc(sizeof(data_column_t),1);

            new_column->type         =tmp_ptr->type;
            new_column->ordinal      =tmp_ptr->ordinal;
            if(tmp_ptr->alias!=0) {
                new_column->alias        =strdup((char*)tmp_ptr->alias);
            }
            if(tmp_ptr->object!=0){
                if(tmp_ptr->type==TOKEN_IDENTIFIER)
                    new_column->object   =duplicate_identifier((identifier_t *)tmp_ptr->object);
                else
                    new_column->object   =strdup((char*)tmp_ptr->object);
            }
            // attach list
            if(new_columns==0){
                new_columns=new_column;
                new_columns->next_tail=new_column;
            } else {
                new_columns->next_tail->next=new_column;
                new_columns->next_tail=new_column;
            }
            tmp_ptr=tmp_ptr->next;
        }
    }
    return new_columns;
}

table_def_t *duplicate_table(table_def_t *table){
    table_def_t *new_table=0;
    if (table){
        new_table=safe_malloc(sizeof(table_def_t),1);
        new_table->columns=duplicate_columns(table->columns);
        new_table->strict=table->strict;
        new_table->file=string_duplicate(table->file);
        new_table->column=string_duplicate(table->column);
        new_table->next=0;
        new_table->tail=0;
        new_table->identifier=duplicate_identifier(table->identifier);
    }
    return new_table;
}


cursor_t *duplicate_cursor(cursor_t *cursor){
    cursor_t *new_cursor=0;
    if (cursor){
        new_cursor=safe_malloc(sizeof(cursor_t),1);
        
        if(cursor->active_table) 
            new_cursor->active_table     =duplicate_table(cursor->active_table);
        
        if(cursor->tables) {
            table_def_t *temp_table=cursor->tables;
            table_def_t *new_table=0;
            while(temp_table){
                if(new_table==0) {
                    new_table=duplicate_table(temp_table);
                } else {
                    new_table->next=duplicate_table(temp_table);
                }
                temp_table=temp_table->next;
            }
            new_cursor->tables           =new_table;
        }
        if(cursor->active_database)
            new_cursor->active_database  =strdup(cursor->active_database);       
        if(cursor->requested_query)
            new_cursor->requested_query  =strdup(cursor->requested_query);       
        if(cursor->executed_query)
            new_cursor->executed_query   =strdup(cursor->executed_query);       
        if(cursor->error_message)
            new_cursor->error_message    =strdup(cursor->error_message);

        new_cursor->parse_position   =cursor->parse_position;       
        new_cursor->error            =cursor->error;       
        new_cursor->status           =cursor->status;       
        new_cursor->created.tv_nsec  =cursor->created.tv_nsec;       
        new_cursor->created.tv_sec   =cursor->created.tv_sec;       
        new_cursor->ended.tv_nsec    =cursor->ended.tv_nsec;       
        new_cursor->ended.tv_sec     =cursor->ended.tv_sec;       
        new_cursor->data_length      =cursor->data_length;;       

    }
    return new_cursor;
}
