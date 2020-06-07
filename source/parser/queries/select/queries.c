#include "../../../include/errors.h"
#include "../../../include/tokens.h"
#include "../../../include/structure.h"
#include "../../../include/debug.h"
#include "../../../include/select.h"

#define EXPRESSION_GROUP_BY 3
#define EXPRESSION_ORDER_BY 4
#define EXPRESSION_COLUMN   5

#define DEFAULT_DATABASE_NAME "this"


int compare_literals(token_t *source,token_t *dest);
void set_error(cursor_t *cursor,int error_no,char *msg);
identifier_t *duplicate_identifier(identifier_t *ident);
data_column_t * duplicate_columns(data_column_t *columns);
table_def_t *duplicate_table(table_def_t *table);

/* Function: duplicate_token
 * -----------------------
 * returns a copy of a token_t from an token_array_t
 * 
 * returns: token_t if the token and index are valid
 *          returns zero (NULL) otherwise
 */
token_t * duplicate_token(token_t *src){
    token_t *dst=safe_malloc(sizeof(token_t),1); 
    dst->depth  =src->depth;
    dst->type   =src->type;
    dst->value  =string_duplicate(src->value);
    for(int i=0;i<TOKEN_MAX_DEPTH;i++) dst->expr[i]=src->expr[i];
    return dst;
}



/* Function: token_at
 * -----------------------------
 * return the token at a given index within a token_array_t
 * 
 * returns: a token if the token index is valid
 *          and the token is not 0 (NULL)
 *          returns zero (NULL) otherwise
 */
token_t * token_at(token_array_t *tokens,int index){
    if(valid_token_index(tokens,index)){
        return &tokens->array[index];
    }
    return 0;
}// end func

/* Function: copy_token_value_at
 * -----------------------------
 * clone a token value
 * 
 * returns: cloned token value if the token index is valid
 *          and the value is non 0 (NULL)
 *          returns zero (NULL) otherwise
 */
char * copy_token_value_at(token_array_t *tokens,int index){
    if(valid_token_index(tokens,index)){
        char *value=tokens->array[index].value;
        if(value) return strdup(value);
    }
    printf("ERROR: COPYING INVALID POSITION %d\n",index);
    return 0;
} // end func

/* Function: add_expr
 * -----------------------------
 * append an expression to another expression
 * 
 * returns: 1 if successfull 
 *          returns zero (NULL) otherwise
 */
int add_expr(expression_t *expression,expression_t *item){
    if(item==0) return 0;

    if(expression->expression==0) {
        expression->expression=item;
        expression->expression_tail=item;
    } else {
        expression->expression_tail->expression=item;
        expression->expression_tail=item;
    }
    return 1;
}

/* Function: process_alias
 * -----------------------
 * returns a alias
 * 
 * returns: cloned token value if:
 *               TOKEN_ALIAS
 *          is matched
 *          index pointer is incremented +1 on match
 *          returns zero (NULL) otherwise
 */
char * process_alias(token_array_t *tokens,int *index){
    char *alias=0;
    switch(token_at(tokens,*index)->type) {
        case TOKEN_ALIAS: alias=copy_token_value_at(tokens,*index); 
                          ++*index; break;
    }
    return alias;
}// end func

/* Function: process_identifier
 * -----------------------
 * returns an identifier_t struct
 * 
 * returns: identifier_t if:
 *            TOKEN_QUALIFIER  or  
 *            TOKEN_SOURCE,TOKEN_QUALIFIER
 *          match is found
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
identifier_t * process_identifier(token_array_t *tokens,int *index){
    identifier_t *ident=0;
    switch(token_at(tokens,*index)->type) {
        case TOKEN_QUALIFIER:   ident=safe_malloc(sizeof(identifier_t),1);
                                ident->qualifier=copy_token_value_at(tokens,*index);
                                ident->source   =copy_token_value_at(tokens,*index+1);
                                *index+=2;
                                break;

        case TOKEN_SOURCE:         
                            ident=safe_malloc(sizeof(identifier_t),1);
                            ident->qualifier=0;
                            ident->source   =copy_token_value_at(tokens,*index);
                            ++*index;
                            break;
    }
    return ident;
} // end func

/* Function: process_litteral
 * -----------------------
 * returns an litteral token match
 * 
 * returns: token_t if :
 *            TOKEN_NULL, TOKEN_HEX, TOKEN_BINARY
 *            TOKEN_STRING, TOKEN_NUMERIC, TOKEN_REAL
 *          match is found
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
token_t * process_litteral(token_array_t *tokens,int *index){
    token_t *token=token_at(tokens,*index);
    token_t *temp_token=0;
    switch(token->type) {
        case TOKEN_NULL   :
        case TOKEN_HEX    :
        case TOKEN_BINARY :
        case TOKEN_STRING :
        case TOKEN_NUMERIC:
        case TOKEN_REAL   : temp_token=duplicate_token(&tokens->array[index]); 
                            if (temp_token) ++*index;
                            return temp_token;
    }
    return 0;
} // end func

/* Function: process_simple_expr
 * -----------------------
 * returns an litteral or identity type with flags
 * 
 * returns: expression_t if :
 *            litteral
 *            identifier_t 
 *          match is found
 *          flags for preapended -+ set
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
expression_t * process_simple_expr(token_array_t *tokens,int *index){
    expression_t *expr=0;
    int mode=0;

    switch(token_at(tokens,*index)->type) {
            case TOKEN_MINUS: mode=-1; ++*index; break;
            case TOKEN_PLUS : mode= 1; ++*index; break;
    }

    token_t *litteral=process_litteral(tokens,index);
    if(litteral) {
        expr=safe_malloc(sizeof(expression_t),1);
        if (mode== 1) expr->positive=1;
        if (mode==-1) expr->negative=1;
        expr->literal=litteral;
        expr->mode=2;
    } else {
        identifier_t *ident=process_identifier(tokens,index);
        if(ident) {
            expr=safe_malloc(sizeof(expression_t),1);
            if (mode== 1) expr->positive=1;
            if (mode==-1) expr->negative=1;
            expr->mode=1;
            expr->identifier=ident;
        }
    }

   return expr;
} // end func

/* Function: process_bit_expr
 * -----------------------
 * match a nested simple expressions with 
 *          bit operation glue or pass a 
 *          simle expression
 * 
 * returns: nested expression_t if a  matched
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
expression_t * process_bit_expr(token_array_t *tokens,int *index){
    expression_t *expr=0;
    expression_t *temp_expr=0;

    expr=process_simple_expr(tokens,index);
    
    if(expr){
        int loop=1;
        while(loop) {
            int operator=token_at(tokens,*index)->type;
            switch(operator) {
                case TOKEN_BIT_OR : 
                case TOKEN_BIT_AND : 
                case TOKEN_SHIFT_LEFT :
                case TOKEN_SHIFT_RIGHT :
                case TOKEN_PLUS : 
                case TOKEN_MINUS : 
                case TOKEN_MULTIPLY :
                case TOKEN_DIVIDE : 
                case TOKEN_MODULUS :  ++*index;
                                    expression_t *expr2=process_simple_expr(tokens,index);
                                    //debug_expr(expr2,10);
                                    if(expr2) expr2->operator=operator;
                                    if(!add_expr(expr,expr2)){
                                        --*index;
                                        loop=0;
                                    } 
                                    break;
                default: loop=0; break;

            }
        }
    }
    return expr;
} // end func

/* Function: process_expr_list
 * -----------------------
 * match a list of simple_expressions
 * 
 * Ex: (x,y,1,'bob',0xFF,0b0101)
 * 
 * returns: nested list of expression_t if matched
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
expression_t * process_expr_list(token_array_t *tokens,int *index){
    expression_t *expr=0;
    int start_point=*index;

    switch(token_at(tokens,*index)->type) {
        case TOKEN_PAREN_LEFT: ++*index;
        default: return 0;
    }

    int loop=1;
    expression_t *temp_expr=0;    
    while(loop){
        
        temp_expr=process_simple_expr(tokens,index);
        if(temp_expr==0) {
            break;
        }
        
        temp_expr->list=1;
        add_expr(expr,temp_expr);
        

        switch(token_at(tokens,*index)->type) {
            case TOKEN_LIST_DELIMITER: ++*index;
            default: loop=0;
        }
    }

    switch(token_at(tokens,*index)->type) {
        case TOKEN_PAREN_RIGHT: ++*index;
        default: *index=start_point; free_expression(expr); return 0;
    }
    
    return expr;
} // end func

/* Function: process_predicate
 * -----------------------
 * match an bit_expression with a "list" or pass 
 *          an bit_expression
 * 
 * returns: nested list of expression_t if matched
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
expression_t * process_predicate(token_array_t *tokens,int *index){
    expression_t *expr=0;

    expr=process_bit_expr(tokens,index);

   /*
    if(expr){
        int mode=0;
        switch(token_at(tokens,*index)->type) {
            case TOKEN_IN     : mode=1;  
            case TOKEN_NOT_IN : mode=-1; 
                                ++*index;
                                  if(add_expr(expr,process_expr_list(tokens,index))){
                                      if(mode== 1) expr->not_in=1;
                                      else if(mode==-1) expr->in=1;
                                  } else {
                                      --*index;
                                  }
                                  break;
        }
    }*/
    return expr;
} // end func

/* Function: process_boolean_primary
 * -----------------------
 * match a nested predicate with comparitor glue
 *          or pass an predicate
 * 
 * returns: nested list of expression_t if matched
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
expression_t * process_boolean_primary(token_array_t *tokens,int *index){
    expression_t *expr=0;
    expr=process_predicate(tokens,index);
    if(expr){
        int token=token_at(tokens,*index)->type;
        switch(token) {
            case TOKEN_IS_NOT_NULL:
            case TOKEN_IS_NULL    : ++*index; expr->comparitor=token; break;
            
            case TOKEN_NULL_EQ    : 
            case TOKEN_LESS_EQ    :
            case TOKEN_GREATER_EQ :
            case TOKEN_LESS       :
            case TOKEN_GREATER    :
            case TOKEN_NOT_EQ     :
            case TOKEN_ASSIGNMENT : ++*index;
                                    expression_t *expr2=process_predicate(tokens,index);
                                    //debug_expr(expr2,10);
                                    if(expr2) expr2->comparitor=token; 
                                    if(add_expr(expr,expr2)){

                                    } else { 
                                        printf("WARNING %d %s %s\n",*index,token_type(token_at(tokens,*index)->type),token_at(tokens,*index)->value);
                                        --*index;
                                    }
                                    break;
        }
    }
    return expr;
} // end func

/* Function: process_expression
 * -----------------------
 * match a nested expression at the TOP MOST LEVEL
 * 
 * returns: nested list of expression_t if matched
 *          index pointer is incremented +n on match
 *          returns zero (NULL) otherwise
 */
expression_t * process_expression(token_array_t *tokens,int *index){
    expression_t *expr=0;
    // NOT
    
    
    int not=0;
    switch(token_at(tokens,*index)->type) {
            case TOKEN_NOT : ++*index; not=1; break;
    }

    expr=process_boolean_primary(tokens,index);
    if(expr) {
        expr->not=not;
        int token=token_at(tokens,*index)->type;
        switch(token) {
            case TOKEN_SHORT_AND :
            case TOKEN_SHORT_OR  :
            case TOKEN_AND       : 
            case TOKEN_OR        : ++*index;
                                if(add_expr(expr,process_expression(tokens,index))){
                                    expr->comparitor=token;
                                } else {
                                    --*index;
                                }
                                break;
        } //end switch
    } //end if

    return expr;
} //end func

/* Function: process_group_column_list
 * -----------------------------
 * gather a group by column list
 * 
 * returns: a nested linked list of 
 *          expression_t if matched
 *          returns zero (NULL) otherwise
 */
expression_t * process_group_column_list(token_array_t *tokens,int *index){
    expression_t *expr=0;
    expression_t *expr2=0;
    identifier_t *ident=0;
    int loop=1;
    while(loop) {
        ident=process_identifier(tokens,index);
        
        if(ident) {
            expr2=safe_malloc(sizeof(expression_t),1); 
            expr2->identifier=ident;
            expr2->mode=EXPRESSION_GROUP_BY;
            if(expr==0) {
                expr=expr2;
            } 
            else {
                add_expr(expr,expr2);
            }

            if(token_at(tokens,*index)->type!=TOKEN_LIST_DELIMITER) {
                loop=0;
            } else {
                ++*index;
            }
        } else {
            loop=0;
        }
    }
    return expr;
}

/* Function: process_order_column_list
 * -----------------------------
 * gather a order by column list
 * 
 * returns: a nested linked list of 
 *          expression_t if matched
 *          returns zero (NULL) otherwise
 */
expression_t * process_order_column_list(token_array_t *tokens,int *index){
    expression_t *expr=0;
    expression_t *expr2=0;
    identifier_t *ident=0;
    int loop=1;
    while(loop) {
        ident=process_identifier(tokens,index);
        if(ident) {
            int token=token_at(tokens,*index)->type;
            switch(token){
                case TOKEN_ASC:  
                case TOKEN_DESC: ++*index; 
                                expr2=safe_malloc(sizeof(expression_t),1); 
                                expr2->direction=token;
                                expr2->identifier=ident;
                                expr2->mode=EXPRESSION_ORDER_BY;
                                if(expr==0) {
                                    expr=expr2;
                                } 
                                else {
                                    add_expr(expr,expr2);
                                }
                                break;
                default: expr2=safe_malloc(sizeof(expression_t),1); 
                                expr2->direction=TOKEN_ASC;
                                expr2->identifier=ident;
                                expr2->mode=EXPRESSION_ORDER_BY;
                                if(expr==0) {
                                    expr=expr2;
                                } 
                                else {
                                    add_expr(expr,expr2);
                                }
                                break;
            }//end switch            
            if(token_at(tokens,*index)->type!=TOKEN_LIST_DELIMITER) {
                loop=0;
            } else {
                ++*index;
            }
        } else {
            loop=0;
        }
    }
    return expr;
}

/* Function: add_data_column
 * -----------------------------
 * append in order to a linked list of data_column_t
 *      this is for building the select colum list
 *      which may be identifiers or litterals
 * 
 * returns: 1 on success
 *             the column object is appended with a new 
 *             data_column_t
 *          returns zero (NULL) otherwise
 */
int add_data_column(data_column_t *column,unsigned int type,void *item,char *alias,int ordinal){
    data_column_t *new_column=safe_malloc(sizeof(data_column_t),1);
    new_column->alias=alias;
    new_column->ordinal=ordinal;
    new_column->object=item;
    new_column->type=type;

    if(column->next==0){
        column->next=new_column;
        column->next_tail=new_column;
        return 1;
    } else {
        column->next_tail->next=new_column;
        column->next_tail=new_column;
    }
    return 1;
}


data_column_t *process_select_list(token_array_t *tokens,int *index){
    // a root object is present so the list is always n+1
    data_column_t *columns=safe_malloc(sizeof(data_column_t),1);
    columns->type=TOKEN_BLANK;

    int            loop   =1;
    int            ordinal=0;
    token_t      * token  =0;
    char         * alias  =0;
    char         * value  =0;
    identifier_t * ident  =0;
    while(loop){
        token=token_at(tokens,*index);

        switch(token->type){
            // litterals
            case TOKEN_STRING:
            case TOKEN_NUMERIC:
            case TOKEN_HEX:
            case TOKEN_BINARY:
            case TOKEN_REAL:
            case TOKEN_NULL: 
                                    value=copy_token_value_at(tokens,*index);
                                    ++*index;
                                    alias=process_alias(tokens,index);
                                    add_data_column(columns,token->type,value,alias,ordinal);
                                    ++ordinal;
                                    break;

            case TOKEN_QUALIFIER:   ident=process_identifier(tokens,index);
                                    alias=process_alias(tokens,index);
                                    add_data_column(columns,TOKEN_IDENTIFIER,ident,alias,ordinal);
                                    ++ordinal;
                                    break;

            case TOKEN_SOURCE:      ident=process_identifier(tokens,index);
                                    alias=process_alias(tokens,index);
                                    add_data_column(columns,TOKEN_IDENTIFIER,ident,alias,ordinal);
                                    ++ordinal;
                                    break;
            default: loop=0; break;
        }
        

        switch(token_at(tokens,*index)->type){
            case TOKEN_LIST_DELIMITER: ++*index;
                                       break;
            default: loop=0;
        }
    } // end while
    return columns;
}

/* Function: process_select
 * -----------------------
 * process
 * 
 * returns: nothing. All output is via stdio
 */
select_t * process_select(token_array_t *tokens,int *start){
    //if(*start>=tokens->top) return;
    int limit_length=0;
    int limit_start=0;
    int loop=1;
    
    
    select_t *select=0;
    

    // switch        
    switch(token_at(tokens,*start)->type){
        case TOKEN_SELECT:  select=safe_malloc(sizeof(select_t),1);
                            select->distinct=0;
                            select->columns=0;
                            select->from=0;
                            select->join=0;
                            select->where=0;
                            select->order=0;
                            select->group=0;
                            select->has_limit_length=0;
                            select->has_limit_start=0;
                            select->limit_start=0;
                            select->limit_length=0;
                            select->column_length=0;
                            select->join_length=0;
                            ++*start; 
                            break;
        default: return 0;
    }//end switch                

    // distinct
    switch(token_at(tokens,*start)->type){
        case TOKEN_DISTINCT: select->distinct=1;         
                             ++*start;
                             break;
    }//end switch                


    select->columns=process_select_list(tokens,start);

    if(select->columns==0) {
        free_select(select);
        return 0;
    }

    // everything after FROM is subordinant to FROM
    // no from.. then we are done;
    // from
    switch(token_at(tokens,*start)->type){
        case TOKEN_FROM:     ++*start;
                            select->from=process_identifier(tokens,start);
                            select->alias=process_alias(tokens,start);
                            break;
        default: return select;
    }// end switch
    
    
    // join
    loop=1;
    while(loop){
        join_t *join;
        switch(token_at(tokens,*start)->type){
            case TOKEN_JOIN:
            case TOKEN_LEFT_JOIN:
            case TOKEN_RIGHT_JOIN:
            case TOKEN_FULL_OUTER_JOIN:
            case TOKEN_INNER_JOIN: 
                                        ++*start;
                                        add_join(select);
                                        join_t *join=&select->join[select->join_length-1];
                                        join->identifier=process_identifier(tokens,start);
                                        join->alias=process_alias(tokens,start);
                                        switch(token_at(tokens,*start)->type){
                                            case TOKEN_ON: ++*start; 
                                                           join->expression=process_expression(tokens,start);
                                                           break;
                                        }//end switch                

                                        
                                        

                                        break;

            default: loop=0; 
                     break;
        }
    }


    // where
    loop=1;
    while(loop){
        switch(token_at(tokens,*start)->type){
            case TOKEN_WHERE: ++*start;
                        select->where=process_expression(tokens,start);
                        break;
            default: loop=0; 
                     break;
        }
    }

    switch(token_at(tokens,*start)->type){
        case TOKEN_GROUP_BY: ++*start; 
                                select->group=process_group_column_list(tokens,start); 
                                break;
    }

    switch(token_at(tokens,*start)->type){
        case TOKEN_ORDER_BY: ++*start; 
                                select->order=process_order_column_list(tokens,start); 
                                break;
    }

    // limit
    loop=1;
    while(loop){
        switch(token_at(tokens,*start)->type){
            case TOKEN_LIMIT_START: select->has_limit_start=1;
                                    select->limit_start=atoi(token_at(tokens,*start)->value);
                                    ++*start;
                                    break;
            case TOKEN_LIMIT_LENGTH: select->has_limit_length=1;
                                     select->limit_length=atoi(token_at(tokens,*start)->value);    
                                     ++*start;
                                     break;
            default: loop=0; break;
        }//end switch
    }
  return select;
}

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
        free_string(ptr->alias);
        tmp_ptr=ptr;
        ptr=ptr->next;
        free(tmp_ptr);
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
        if(table_def->columns   ) free_expression(table_def->columns);
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
    if(cursor->tables)  free_table_def(cursor->tables);
    if(cursor->active_table)  free_table_def(cursor->active_table);
    
    if(cursor) free(cursor);
    return 1;
}

/* Function: select_print
 * -----------------------
 * visibly print the select data structure
 * 
 * returns: nothing. All output is via stdio
 */
void select_print(select_t *select){
    // DEBUGGING INFORMATION

    if(select==0) return;
    printf("SELECT\n");
    if (select->distinct) printf("HAS DISTINCT\n");
    if (select->columns){
        data_column_t * next=select->columns;
        // skip root element;
        if(next) next=next->next;
        
        while(next){
            if(next->object==0) printf ("Missing object in datacolumn \n");
            else 
            switch(next->type){

                case TOKEN_STRING:
                case TOKEN_NUMERIC:
                case TOKEN_HEX:
                case TOKEN_BINARY:
                case TOKEN_REAL:
                case TOKEN_NULL: 
                
                 printf("%s ",  token_type(next->type));
                 printf("%s ", (char*)next->object);
                 printf("%s ",  next->alias);
                 printf("%d\n",  next->ordinal);
                                  break;
                case TOKEN_IDENTIFIER: printf("%s- %s.%s ALIAS %s, %d\n",token_type(next->type),
                                                            ((identifier_t *)next->object)->qualifier ,
                                                            ((identifier_t *)next->object)->source ,
                                                            next->alias ,
                                                            next->ordinal );
                                    break;
                default:   printf("%s \n",token_type(next->type));
                            break;
            }//end switch
            next=next->next;
        }//end while
    } else {
        printf("  NO COLUMNS\n");
    }
   

     if (select->from) {
        printf("FROM\n");
        if(select->from->qualifier) {
            printf("%s.",select->from->qualifier);
        }
        if(select->from->source) {
            printf("%s",select->from->source);
            if(select->alias) printf(" ALIAS: %s ",select->alias);
            printf("\n");
        }
        
    }

    if (select->join) {
        printf("JOIN %d\n",select->join_length);
        for(int i=0;i<select->join_length;i++){
            if(select->join[i].identifier) {
                if(select->join[i].identifier->qualifier) {
                    printf("%s.",select->join[i].identifier->qualifier);
                }
                if(select->join[i].identifier->source) {
                    printf("%s ",select->join[i].identifier->source);
                }
                if(select->join[i].alias) printf("ALIAS: %s",select->join[i].alias);
                printf("\n");
            }
            debug_expr(select->join[i].expression,0);
        }
        
    }
    if(select->where) {
        printf(" ---WHERE---");
        debug_expr(select->where,0);
    }
    if(select->group) {
        printf(" ---GROUP---");
        debug_expr(select->group,0);
    }
    if(select->order) {
        printf(" ---ORDER---");
        debug_expr(select->order,0);
    }


    if (select->has_limit_start) printf("LIMIT_START:   %d\n",select->limit_start);
    if (select->has_limit_length) printf("LIMIT_LENGTH : %d\n",select->limit_length);
}

/* Function: debug_expr
 * -----------------------
 * visibly print the nested expresison_t data structure
 * 
 * returns: nothing. All output is via stdio
 */
void debug_expr(expression_t *expr,int depth){
    if(expr==0) {
        printf ("Expression NULL\n");
        return;
    }
    char *pad="";
    if(depth>0) pad=safe_malloc(depth+1,1);

    for(int i=0;i<depth;i++) pad[i]=' ';

    printf("%s- expr:\n",pad);
    printf("%s  - mode:   %d ",pad,expr->mode);
    printf("%s  - list:   %d ",pad,expr->list);
    printf("%s  - not:    %d ",pad,expr->not);
    printf("%s  - not_in: %d ",pad,expr->not_in);
    printf("%s  - in:     %d \n",pad,expr->in);
    printf("%s  - direction:  %s ",pad,token_type(expr->direction));
    printf("%s  - negative:   %d ",pad,expr->negative);
    printf("%s  - positive:   %d ",pad,expr->positive);
    printf("%s  - comparitor: %s ",pad,token_type(expr->comparitor));
    printf("%s  - operator:   %s \n",pad,token_type(expr->operator));
    if(expr->identifier){
        printf("%s - Identifier: %s.%s\n",pad,expr->identifier->qualifier,expr->identifier->source);
    }
    if(expr->literal) {
        printf("%s - Litteral: [%s] '%s'\n",pad,token_type(expr->literal->type),expr->literal->value);
    }

    if(depth>0) free(pad);
    if(expr->expression) debug_expr(expr->expression,depth+1);
    printf("\n");

}

/* Function: process_column_list
 * -----------------------
 * process table column definitions list (for group by)
 * 
 * returns: a nested set of expression_t
 *          0 or (NULL) on failure
 */
expression_t * process_column_list(token_array_t *tokens,int *index){

    expression_t *expr=0;
    expression_t *expr2=0;

    switch(token_at(tokens,*index)->type) {
        case TOKEN_PAREN_LEFT: ++*index; 
                               break;
        default: return 0;
    }

    int loop=1;
    while(loop) {
        token_t *column=0;
        if(token_at(tokens,*index)->type==TOKEN_STRING) {
            column=duplicate_token(&tokens->array[index]);
            ++*index;
        }
        if(column) {
            expr2=safe_malloc(sizeof(expression_t),1); 
            expr2->literal=column;
            expr2->mode=EXPRESSION_COLUMN;
            if(expr==0) {
                expr=expr2;
            } 
            else {
                add_expr(expr,expr2);
            }

            if(token_at(tokens,*index)->type!=TOKEN_LIST_DELIMITER) {
                loop=0;
            } else {
                ++*index;
            }
        } else {
            loop=0;
        }
    }
    switch(token_at(tokens,*index)->type) {
        case TOKEN_PAREN_RIGHT: ++*index; 
                                break;
        default: free_expression(expr); 
                 return 0;
    }
    
    return expr;
}

/* Function: process_create_table
 * -----------------------
 * parse sql for table creation and return a data structure
 * 
 * returns: table_def_t data structure
 *          0 (NULL) for failure
 */
table_def_t * process_create_table(token_array_t *tokens,int *start){
    table_def_t *table_def=0;
   
    // required
    switch(token_at(tokens,*start)->type){
        case TOKEN_CREATE_TABLE: ++*start; 
                                 table_def=safe_malloc(sizeof(table_def_t),1);
                                 break;
        default: return 0;
    }//end switch                

    // required
    table_def->identifier=process_identifier(tokens,start);
    if(table_def->identifier==0) {
        free_table_def(table_def); 
        return 0;
    }

    // required
    
    table_def->columns=process_column_list(tokens,start);
    if(table_def->columns==0) {
        free_table_def(table_def); 
        return 0; 
    }

    // required
    if(token_at(tokens,*start)->type==TOKEN_FILE   && 
       token_at(tokens,*start+1)->type==TOKEN_STRING) { 
           ++*start; 
           table_def->file= copy_token_value_at(tokens,*start); 
           ++*start;
       }
    else {
        free_table_def(table_def); 
        return 0;
    }
    /*
    // optional
    if(token_at(tokens,*start)->type==TOKEN_REPO   && 
       token_at(tokens,*start+1)->type==TOKEN_STRING) { 
           ++*start; 
           table_def->repo= copy_token_value_at(tokens,*start); 
           ++*start; 
        }
*/
    // optional
    if(token_at(tokens,*start)->type==TOKEN_COLUMN && 
       token_at(tokens,*start+1)->type==TOKEN_STRING) { 
           ++*start; 
           table_def->column= copy_token_value_at(tokens,*start); 
           ++*start;
        } // optional

    // optional
    if(token_at(tokens,*start)->type==TOKEN_STRICT) {
        ++*start;
        token_t *t1=token_at(tokens,*start);
        if(t1->type==TOKEN_TRUE)  { table_def->strict=1; ++*start; }
        else if(t1->type==TOKEN_FALSE) { table_def->strict=0; ++*start; } 
    }
    
        
    return table_def;
}

/* Function: debug_identifier
 * -----------------------
 * visibly print the identifier_t data structure
 * 
 * returns: nothing
 */
void debug_identifier(identifier_t *identifier) {
    printf ("  Identifier: %s.%s\n",identifier->qualifier,identifier->source);

}

/* Function: debug_create_table
 * -----------------------
 * visibly print the tabel_def data structure
 * 
 * returns: nothing
 */
void debug_create_table(table_def_t *table) {
    if(table==0) {
        printf ("Cant debug create table. Null\n");
    } 
    printf (" -- CREATE_TABLE DEBUG -------------\n");
    expression_t *temp_ptr=table->columns;
    debug_identifier(table->identifier);
    
    while(temp_ptr){
        printf("  Column : %s\n",temp_ptr->literal->value);
        temp_ptr=temp_ptr->expression;
    } 

    /*printf("base:          %s \n",table->base);
    printf("fifo:          %s \n",table->fifo);
    printf("repo:          %s \n",table->repo);
    printf("url:           %s \n",table->url);
    printf("account:       %s \n",table->account);
    printf("password:      %s \n",table->password);
    printf("repo_path:     %s \n",table->repo_path);
    printf("repo_base:     %s \n",table->repo_base);
    printf("push_on_commit:%d \n",table->push_on_commit);
    printf("pull_on_read:  %d \n",table->pull_on_read);
    */
    printf("  file:          %s \n",table->file);
    printf("  column:        %s \n",table->column);
    printf("  strict:        %d \n",table->strict);
    printf (" --\n");
}



/* Function: validate_select
 * -----------------------
 * validate a select structures logic
 * 
 * returns: nothing
 */
int validate_select(select_t *select){

    return 0;
}
/*
typedef struct data_set{
    column_t columns;
    rows_t data;

}
*/
/* Function: validate_create_table
 * -----------------------
 * validate a create_table structures logic
 *
 * fail if:  
 *   if the table exists in the curent global memory
 * returns: 1 for success
 *          zero or null otherwise
 */
int fixup_create_table(cursor_t *cursor,table_def_t *table){
    if(cursor->active_table==0) {
        ghost(ERR_NO_TABLE_SELECTED);  
    }
    

    return 1;
}

int compare_identifiers(identifier_t *source,identifier_t *dest){
    if (strcmp(source->qualifier,dest->qualifier)==0 && 
        strcmp(source->source,dest->source)==0) return 1;
    return 0;
}

int compare_literals(token_t *source,token_t *dest){
    if (strcmp(source->value,dest->value)==0) return 1;
    return 0;
}

/* Function: validate_create_table
 * -----------------------
 * validate a create_table structures logic
 *
 * fail if:  
 *   if the table exists in the curent global memory
 * returns: 1 for success
 *          zero or null otherwise
 */
int validate_create_table(cursor_t * cursor,table_def_t *table){
    /*
    table->column;
    table->columns;
    table->file;
    table->identifier;
    table->strict;
    */

    char *msg=0;    
    table_def_t *next=cursor->tables;
    // the source will always be available. this is caught in the parsing phase
    // the db may not be set.
    if(table->identifier->qualifier==0) {
        table->identifier->qualifier=get_current_database(cursor);
    }
    // check to see if table exists
    while(next){
        if(next->identifier) {
            if(compare_identifiers(next->identifier,table->identifier)){
                msg=safe_malloc(1000,1);
                sprintf(msg,"Table already exists %s.%s",table->identifier->qualifier,table->identifier->source);
                set_error(cursor,ERR_TABLE_ALREADY_EXISTS,msg);
                return 0;
            }
        }
        next=next->next;
    }
    // check to see if file is accessable
    if( access( table->file, F_OK) != -1 ) {
        if( access( table->file, R_OK) != -1 ) {
            if( access( table->file, W_OK) != -1 ) {
            } else {
                msg=safe_malloc(1000,1);
                sprintf(msg,"Cant write to file %s",table->file);
                set_error(cursor,ERR_FILE_WRITE_PERMISSION,msg);
                return 0;
            }
        } else {
            msg=safe_malloc(1000,1);
            sprintf(msg,"Cant read from file %s",table->file);
            set_error(cursor,ERR_FILE_READ_PERMISSION,msg);
            return 0;
        }
    } else {
        msg=safe_malloc(1000,1);      
        sprintf(msg,"Cant find file %s",table->file);
        set_error(cursor,ERR_FILE_NOT_FOUND,msg);
        return 0;
    }
    //check to see if table has columns and they are uniquely named

    if(table->columns==0) {
        msg=safe_malloc(1000,1);      
        sprintf(msg,"no columns in %s.%s",table->identifier->qualifier,table->identifier->source);
        set_error(cursor,ERR_TABLE_HAS_NO_COLUMNS,msg);
        return 1;
    }
    expression_t *outer_tmp=table->columns;
    expression_t *inner_tmp;
    int outer_index=0;
    int inner_index=0;
    while(outer_tmp){
        if(outer_tmp->literal) {
            inner_tmp=table->columns;
            inner_index=0;
            while(inner_tmp){
                // skip itself
                if(inner_index!=outer_index) {
                    if(compare_literals(outer_tmp->literal,inner_tmp->literal)) {
                        msg=safe_malloc(1000,1);      
                        sprintf(msg,"Column must be a unique literal %s",inner_tmp->literal->value);
                        set_error(cursor,ERR_AMBIGUOUS_COLUMN_NAME,msg);
                        return 0;
                    }
                }
                ++inner_index;
                inner_tmp=inner_tmp->expression;
            }

        } else {
            msg=safe_malloc(1000,1);      
            sprintf(msg,"Column must be a unique literal");
            set_error(cursor,ERR_INVALID_COLUMN_NAME,msg);
            return 0;
        }
        outer_tmp=outer_tmp->expression;
        ++outer_index;
    }


    // set most recent addition to active. append to end of list. update next and tail
    if(cursor->tables==0)  {
        cursor->tables=table;
        cursor->tables->tail=table;
        cursor->active_table=table;
    } else {
        cursor->tables->tail->next=table;
        cursor->tables->tail=table;
        cursor->active_table=table;
    }

    return 1;
}


cursor_t *init_cursor(){
    cursor_t * cursor=safe_malloc(sizeof(cursor_t),1);
    cursor->data_length=0;
    clock_gettime(CLOCK_REALTIME,&cursor->created);
    return cursor;
}

void debug_cursor(cursor_t *cursor){

    printf("\n# Cursor\n");

    printf("- Active database: %s\n", get_current_database(cursor) );
    printf("- Created: %s", ctime(&cursor->created));
    printf("- Ended: %s", ctime(&cursor->ended));
    printf("- Data Length: %d\n", cursor->data_length);
    
    printf("- Ellapsed Time: %ld.%09ld\n", (long)(cursor->ended.tv_sec - cursor->created.tv_sec),
        cursor->ended.tv_nsec - cursor->created.tv_nsec);
        
    int                 data_length;
    if(cursor->status){
        printf("- Status: SUCCESS\n");
    } else {
        printf("- Status: FAILURE\n");
    }

    if(cursor->requested_query) {
        printf("- Resuested: %s\n",cursor->requested_query);
        if(cursor->executed_query) {
            printf("- Executed: %s\n",cursor->executed_query);
        }
    }
    if(cursor->error) {
        printf("- ERROR NUM: %d %s\n",cursor->error,vomit(cursor->error));
        printf("- ERROR: %s\n",cursor->error_message);
    }

}

char *get_current_database(cursor_t *cursor){
    // always set.. defaults to "information_schema"
    if (cursor->active_table==0) return DEFAULT_DATABASE_NAME;
    return cursor->active_table->identifier->qualifier;
}

void set_error(cursor_t *cursor,int error_no,char *msg){
    if(cursor->error_message) {
        free(cursor->error_message);
    }
    cursor->error=error_no;
    cursor->error_message=msg;
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
            new_column->alias    =string_duplicate(tmp_ptr->alias);
            new_column->ordinal  =tmp_ptr->ordinal;
            new_column->type     =tmp_ptr->type;
            
            if(tmp_ptr->type==EXPRESSION_COLUMN) {
                new_column->object   =duplicate_token((token_t*)tmp_ptr->object);
            }
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