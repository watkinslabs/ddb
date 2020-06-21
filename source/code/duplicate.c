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
            new_column->alias        =strdup((char*)tmp_ptr->alias);
            if(tmp_ptr->type==TOKEN_IDENTIFIER)
                new_column->object   =duplicate_identifier((identifier_t *)tmp_ptr->object);
            else
                new_column->object   =strdup((char*)tmp_ptr->object);

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
