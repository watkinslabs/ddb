#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"


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
char * process_alias(cursor_t *cursor,token_array_t *tokens,int *index){
    char *alias=0;
    token_t *temp_token=token_at(tokens,*index);
    if(temp_token==0) return 0;

    switch(temp_token->type) {
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
identifier_t * process_identifier(cursor_t *cursor,token_array_t *tokens,int *index){
    identifier_t *ident=0;
    token_t *temp_token=token_at(tokens,*index);
    if(temp_token==0) return 0;
    
    switch(temp_token->type) {
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
token_t * process_litteral(cursor_t *cursor,token_array_t *tokens,int *index){
    token_t *token=token_at(tokens,*index);
    if(token==0) return 0;

    token_t *temp_token=0;
    switch(token->type) {
        case TOKEN_NULL   :
        case TOKEN_HEX    :
        case TOKEN_BINARY :
        case TOKEN_STRING :
        case TOKEN_NUMERIC:
        case TOKEN_REAL   : temp_token=duplicate_token(&tokens->array[*index]); 
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
expression_t * process_simple_expr(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *expr=0;
    int mode=0;
    int position=*index;

    token_t *temp_token=token_at(tokens,*index);
    if(temp_token==0) return expr;

    switch(temp_token->type) {
            case TOKEN_MINUS: mode=TOKEN_MINUS; ++*index; break;
            case TOKEN_PLUS : mode=TOKEN_PLUS; ++*index; break;
    }

    token_t *litteral=process_litteral(cursor,tokens,index);
    if(litteral) {
        expr=safe_malloc(sizeof(expression_t),1);
        if (mode!=0) expr->uinary_operator=mode;
        expr->literal=litteral;
        expr->mode=2;
        
        if (mode!=0) {
            switch(litteral->type){
                case TOKEN_NUMERIC:
                case TOKEN_HEX:
                case TOKEN_BINARY:
                case TOKEN_REAL: break;
                default:  *index=position+1;
                        free(expr);
                        return 0;
            }
        }
    } else {
        identifier_t *ident=process_identifier(cursor,tokens,index);
        if(ident) {
            expr=safe_malloc(sizeof(expression_t),1);
            if (mode!=0) expr->uinary_operator=mode;
            expr->mode=1;
            expr->identifier=ident;
        }
    }
    if(expr==0 && mode!=0) {
        --*index;
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
expression_t * process_bit_expr(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *expr=0;
    expression_t *temp_expr=0;

    expr=process_simple_expr(cursor,tokens,index);
    //if(expr)
    //debug_expr(expr,10);
    if(expr){
        int loop=1;
        while(loop) {
            token_t *temp_token=token_at(tokens,*index);
            if(temp_token==0) return expr;
            switch(temp_token->type) {
                case TOKEN_BIT_OR : 
                case TOKEN_BIT_AND : 
                case TOKEN_SHIFT_LEFT :
                case TOKEN_SHIFT_RIGHT :
                case TOKEN_PLUS : 
                case TOKEN_MINUS : 
                case TOKEN_MULTIPLY :
                case TOKEN_DIVIDE : 
                case TOKEN_MODULUS :  ++*index;
                                    expression_t *expr2=process_simple_expr(cursor,tokens,index);
                                    //debug_expr(expr2,10);
                                    if(expr2) expr2->arithmetic_operator=temp_token->type;
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
expression_t * process_expr_list(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *expr=0;
    int start_point=*index;

    token_t *temp_token=token_at(tokens,*index);
    if(temp_token==0) return 0;

    switch(temp_token->type) {
        case TOKEN_PAREN_LEFT: ++*index;
        default: return 0;
    }

    int loop=1;
    expression_t *temp_expr=0;    
    while(loop){
        
        temp_expr=process_simple_expr(cursor,tokens,index);
        if(temp_expr==0) {
            break;
        }
        
        temp_expr->list=1;
        add_expr(expr,temp_expr);
        
        temp_token=token_at(tokens,*index);
        if(temp_token==0) return 0;

        switch(temp_token->type) {
            case TOKEN_LIST_DELIMITER: ++*index;
            default: loop=0;
        }
    }

    temp_token=token_at(tokens,*index);
    if(temp_token==0) return 0;
    switch(temp_token->type) {
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
expression_t * process_predicate(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *expr=0;

    expr=process_bit_expr(cursor,tokens,index);

   /*
    if(expr){
        int mode=0;
        switch(cursor_t *cursor,token_at(tokens,*index)->type) {
            case TOKEN_IN     : mode=1;  
            case TOKEN_NOT_IN : mode=-1; 
                                ++*index;
                                  if(add_expr(expr,process_expr_list(cursor,tokens,index))){
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
expression_t * process_boolean_primary(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *expr=0;
    expr=process_predicate(cursor,tokens,index);
    if(expr){
        token_t *temp_token=token_at(tokens,*index);
        if(temp_token==0) return expr;
        switch(temp_token->type) {
            case TOKEN_IS_NOT_NULL:
            case TOKEN_IS_NULL    : ++*index; expr->comparison_operator=temp_token->type; 
                                    break;
            
            case TOKEN_NULL_EQ    : 
            case TOKEN_LESS_EQ    :
            case TOKEN_GREATER_EQ :
            case TOKEN_LESS       :
            case TOKEN_GREATER    :
            case TOKEN_NOT_EQ     :
            case TOKEN_ASSIGNMENT : ++*index;
                                    expression_t *expr2=process_predicate(cursor,tokens,index);
                                    //debug_expr(expr2,10);
                                    if(expr2) expr2->comparison_operator=temp_token->type; 
                                    if(!add_expr(expr,expr2)){
                                        token_t *temp_token2=token_at(tokens,*index);
                                        if(temp_token2) {
                                            printf("WARNING %d %s %s\n",*index,token_type(temp_token2->type),temp_token2->value);
                                        } else {
                                            return 0;
                                        }
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
expression_t * process_expression(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *temp_expr=0;
    expression_t *expr=0;
    int start_loop=1;
    // NOT

    int needs_expression=0;    
    int pos=0;
    while(1) {
        // check for aNOT clause
        int not=0;
        token_t *temp_token=token_at(tokens,*index);
        if(temp_token==0) {
            return expr;
        }
       
        switch(temp_token->type) {
                case TOKEN_NOT : ++*index; not=1; break;
        }

        pos=*index;
        // pull an expression 
        temp_expr=process_boolean_primary(cursor,tokens,index);

        // first run..store root
        if(start_loop==1) {
            expr=temp_expr;
        }
        //printf("---EXPR\n");
        //debug_expr(temp_expr,10);

        // nothing returned.. eject with curent list
        if(!temp_expr) {
            return expr;
        }
        if(start_loop!=1){
            if(!add_expr(expr,temp_expr)){
                *index=pos;
                return expr;
            }
        }
        //continue;
        temp_token=token_at(tokens,*index);
        if(temp_token==0) {
            break;
        }
        switch(temp_token->type) {
            case TOKEN_SHORT_AND :
            case TOKEN_SHORT_OR  :
            case TOKEN_AND       : 
            case TOKEN_OR        : ++*index;
                                  temp_expr->not=not;
                                  expression_t *temp_expr2=safe_malloc(sizeof(expression_t),1);
                                  temp_expr2->mode=6;
                                  temp_expr2->logical_operator=temp_token->type;
                                  add_expr(expr,temp_expr2);
                                  break;
            default: break;
        } //end switch

        start_loop=0;
    }
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
expression_t * process_group_column_list(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *expr=0;
    expression_t *expr2=0;
    identifier_t *ident=0;
    int loop=1;
    while(loop) {
        ident=process_identifier(cursor,tokens,index);
        
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
            token_t *temp_token=token_at(tokens,*index);
            if(temp_token==0){
                loop=0;
            } else {
                if(temp_token->type!=TOKEN_LIST_DELIMITER) {
                    loop=0;
                } else {
                    ++*index;
                }
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
expression_t * process_order_column_list(cursor_t *cursor,token_array_t *tokens,int *index){
    expression_t *expr=0;
    expression_t *expr2=0;
    identifier_t *ident=0;
    int loop=1;
    while(loop) {
        ident=process_identifier(cursor,tokens,index);
        if(ident) {
            token_t *temp_token=token_at(tokens,*index);
            if(temp_token==0) {
                return expr;
            }
            
            switch(temp_token->type){
                case TOKEN_ASC:  
                case TOKEN_DESC: ++*index; 
                                expr2=safe_malloc(sizeof(expression_t),1); 
                                expr2->direction=temp_token->type;
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
            
            temp_token=token_at(tokens,*index);
            if(temp_token==0) {
                return expr;
            }
            
            if(temp_token->type!=TOKEN_LIST_DELIMITER) {
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

data_column_t * process_select_list(cursor_t *cursor,token_array_t *tokens,int *index){
    // a root object is present so the list is always n+1
    data_column_t *columns=0;
    

    int            loop   =1;
    int            ordinal=0;
    token_t      * token  =0;
    char         * alias  =0;
    char         * value  =0;
    identifier_t * ident  =0;
    while(loop){
        token=token_at(tokens,*index);
        if(token==0) return columns;

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
                                    alias=process_alias(cursor,tokens,index);
                                    columns=add_data_column(columns,token->type,value,alias,ordinal);
                                    ++ordinal;
                                    break;

            case TOKEN_QUALIFIER:   ident=process_identifier(cursor,tokens,index);
                                    alias=process_alias(cursor,tokens,index);
                                    columns=add_data_column(columns,TOKEN_IDENTIFIER,ident,alias,ordinal);
                                    ++ordinal;
                                    break;

            case TOKEN_SOURCE:      ident=process_identifier(cursor,tokens,index);
                                    alias=process_alias(cursor,tokens,index);
                                    columns=add_data_column(columns,TOKEN_IDENTIFIER,ident,alias,ordinal);
                                    ++ordinal;
                                    break;
            default: loop=0; break;
        }
        
        token=token_at(tokens,*index);
        if(token==0) {
            return columns;
        }
            
        switch(token->type){
            case TOKEN_LIST_DELIMITER: ++*index;
                                       break;
            default: loop=0;
        }
    } // end while
    return columns;
}

/* Function: process_column_list
 * -----------------------
 * process table column definitions list for create table
 * 
 * returns: a nested set of expression_t
 *          0 or (NULL) on failure
 */
data_column_t * process_column_list(cursor_t *cursor,token_array_t *tokens,int *index){

    data_column_t * col=0;
    int ordinal=0;
    token_t *temp_token=token_at(tokens,*index);
    if(temp_token==0) {
        return 0;
    }

    switch(temp_token->type) {
        case TOKEN_PAREN_LEFT: ++*index; 
                               break;
        default: return 0;
    }

    int loop=1;
    while(loop) {
        token_t *column=0;
        temp_token=token_at(tokens,*index);
        if(temp_token==0) break;
        if(temp_token->type==TOKEN_STRING) {
            column=duplicate_token(&tokens->array[*index]);
            ++*index;
        }
        if(column) {
            col=add_data_column(col,column->type,column->value,0,ordinal);
            //freeing the wrapper but keeping the value... freed later by other func
            free(column);
            ++ordinal;
            temp_token=token_at(tokens,*index);
            if(temp_token==0) break;
            if(temp_token->type!=TOKEN_LIST_DELIMITER) {
                loop=0;
            } else {
                ++*index;
            }
        } else {
            loop=0;
        }
    }
    temp_token=token_at(tokens,*index);
    if(temp_token && temp_token->type==TOKEN_PAREN_RIGHT) {
         ++*index; 
    } else {
         free_data_columns(col); 
        return 0;
    }
    
    return col;
}

