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

use_t *process_use(token_array_t *tokens,int *start){
    use_t *use=0;
   
    // required
    switch(token_at(tokens,*start)->type){
        case TOKEN_USE: ++*start; 
                                 use=safe_malloc(sizeof(use_t),1);
                                 break;
        default: return 0;
    }//end switch                

    // required

    // SOURCE is a reqwite by the lexer. while not perfect its always accurate. 
    // it should be qualifier...
    switch(token_at(tokens,*start)->type) {
        case TOKEN_SOURCE:  use->database=copy_token_value_at(tokens,*start);
                            ++*start;
                            break;
    }
    if(use->database==0) {
        free_use(use); 
        return 0;
    }
    return use;
}
