#include "include/free.h"

/* Function: free_data_columns
 * -----------------------
 * free the nested resources in a data_column_t linked list
 * 
 * returns: 1 on success
 *          zero or NULL otherwise
 */
int free_data_columns(data_column_t *columns){
    data_column_t *ptr=columns;
    data_column_t *tmp_ptr=0;
    
    while(ptr){
        switch(ptr->type){
            case TOKEN_STRING:
            case TOKEN_NUMERIC:
            case TOKEN_HEX:
            case TOKEN_BINARY:
            case TOKEN_REAL:
            case TOKEN_NULL: if(ptr->object) free(ptr->object); 
                             break;
            case TOKEN_IDENTIFIER: free_ident(ptr->object); 
            break;
        }
        if(ptr->alias) {
            free(ptr->alias);
        }
        tmp_ptr=ptr;
        ptr=ptr->next;
        free(tmp_ptr);
    }
    return 1;
}

int free_column_list(char ** columns,int length) {
    if(columns) {
        for(int i=0;i<length;i++){
            if(columns[i]){
                free(columns[i]);
            }
        }
        free(columns);
    }
    return 1;
}

int free_data_set(data_set_t *data_set){
    if(data_set){
        if(data_set->rows) {
            for(int i=0;i<data_set->row_length;i++){
                for(long c=0;c<data_set->rows[i]->column_length;c++)
                    free(data_set->rows[i]->columns[c]);
                free(data_set->rows[i]->columns);
                free(data_set->rows[i]);
            }
            free(data_set->rows);
        }
        for(int i=0;i<data_set->column_length;i++){
            free(data_set->columns[i]);
        }
        free(data_set->columns);
        //
        free(data_set);
    }
    return 1;
}

/* Function: select_free
 * -----------------------------
 * free the data structure of a select_t
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_select(select_t *select) {
    // free resources
    free_data_columns(select->columns);
    if(select->from) {
        free_ident(select->from);
        free_string(select->alias);
    }
    if(select->join) {
        for(int i=0;i<select->join_length;i++) {
            free_ident(select->join[i].identifier);
            free_expression(select->join[i].expression);
            free_string(select->join[i].alias);
        }
        free(select->join);
    }
    free_expression(select->where);
    free_expression(select->group);
    free_expression(select->order);
    free(select);
    return 0;
}

/* Function: free_string
 * -----------------------------
 * free the data structure of a char*
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_string(char *data){
    if(data) free(data);
 return 1;
}

/* Function: free_expression
 * -----------------------------
 * free the data structure of a expression_t
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_expression(expression_t *expr){
 expression_t *expr_ptr=expr;
 while(expr_ptr){
     if(expr_ptr->literal) free_litteral(expr_ptr->literal);
     if(expr_ptr->identifier) free_ident(expr_ptr->identifier);
     expression_t * last=expr_ptr;
     expr_ptr=expr_ptr->expression;
     free(last);
 }
 return 1;
}

int free_expression_value(expression_value_t *expr){
    if(expr) {
        //if(expr->STRING_V) free(&expr->STRING_V);
        free(expr);
        return 1;
    }
    return 0;
}

/* Function: free_ident
 * -----------------------------
 * free the data structure of a identifier_t
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_ident(identifier_t *ident){
    if(ident){
        if(ident->qualifier) {
            free(ident->qualifier);
        }
        if(ident->source) {
            free(ident->source);
        }
        free(ident);
    }
    return 1;
}

/* Function: free_litteral
 * -----------------------------
 * free the data structure of a token_t
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_litteral(token_t *token){
    if(token){
        if(token->value) {
            free(token->value);
        }
        free(token);
    }
    return 1;
}

/* Function: free_table_def
 * -----------------------
 * free the table_def_t structure
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_table_def(table_def_t *table_def){
    if(table_def){
        if(table_def->columns   ) free_data_columns(table_def->columns);
        if(table_def->column    ) free_string(table_def->column);
        if(table_def->identifier) free_ident(table_def->identifier);
        /*
        if(table_def->url       ) free(table_def->url       );
        if(table_def->repo_path ) free(table_def->repo_path );
        if(table_def->repo_base ) free(table_def->repo_base );
        if(table_def->repo      ) free(table_def->repo      );
        if(table_def->password  ) free(table_def->password  );
        if(table_def->fifo      ) free(table_def->fifo      );*/
        if(table_def->file      ) free(table_def->file      );
        if(table_def->next) free_table_def(table_def->next);
        free(table_def);
        table_def=0;
    } 
    return 1;
}

/* Function: free_cursor
 * -----------------------
 * free the cursor_t structure, the 
 *   root of all query operations
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_cursor(cursor_t *cursor){
    if(cursor->error_message  ) free_string(cursor->error_message);
    if(cursor->executed_query ) free_string(cursor->executed_query);
    if(cursor->requested_query) free_string(cursor->requested_query);
    if(cursor->tables)          free_table_def(cursor->tables);
    if(cursor->active_table)    cursor->active_table=0;
    if(cursor->active_database) free(cursor->active_database);
    if(cursor->results)         free_data_set(cursor->results);
    if(cursor->source_alias) {
        for(int i=0;i<cursor->source_count;i++) free(cursor->source_alias[i]);
        free(cursor->source_alias);
    }
    if(cursor->identifier_lookup) {
        //loop
        free(cursor->identifier_lookup);
    }
    if(cursor) free(cursor);
    return 1;
}

/* Function: free_use
 * -----------------------
 * free the uset_t structure, the 
 *   changes the active database
 * 
 * returns: 1 for success or 0 (NULL) for failure
 */
int free_use(use_t *use){
    if(use){
        if(use->database) free(use->database);
        free(use);
    }
    return 1;
}

int free_token(token_t *token){
    if(token) {
        if(token->value) free (token->value);
        free(token);
    }
    return 1;
}