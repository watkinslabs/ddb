
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <math.h>
#include <assert.h>

#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"
#include "../include/queries.h"

#define LINE_ENDING '\n'
#define DOUBLE_QUOTE '\"'
#define SINGLE_QUOTE '\''

data_set_t * load_file(cursor_t *cursor,identifier_t *table_ident);
data_set_t * new_data_set(char **columns,int column_count,int row_count);
char ** get_column_list(data_column_t *columns,int length);

char * long_2_string(long value);
char * int_2_string(int value);
char * float_2_string(float  value);
int    compare_expression_value(expression_value_t *e1,expression_value_t *e2,int comparison);
long   return_match(cursor_t *cursor,select_t *select,int set);

/* Function: validate_create_table
 * -----------------------
 * execute the creatiopn of a table in the cuyrent cursor
 *
 * fail if:  
 *   null
 * returns: 1 for success
 *          zero or null otherwise
 */
int execute_create_table(cursor_t * cursor,table_def_t *table){
    
    // set most recent addition to active. append to end of list. update next and tail
    table_def_t *new_table=duplicate_table(table);
    if(cursor->tables==0)  {
        cursor->tables=new_table;
        cursor->tables->tail=new_table;
        cursor->active_table=new_table;
    } else {
        cursor->tables->tail->next=new_table;
        cursor->tables->tail=new_table;
        cursor->active_table=new_table;
    }

    return 1;
}

/* Function: execute_use
 * -----------------------
 * sets the curent database in the active cursor
 *
 * fail if:  
 *   null
 * returns: 1 for success
 *          zero or null otherwise
 */
int execute_use(cursor_t *cursor,use_t *use){
    if(use){
        // cleanup prior name
        if(cursor->active_database) free_string(cursor->active_database);
        // set curent name
        cursor->active_database=string_duplicate(use->database);
        return 1;
    }
    return 0;
}


//get the value of the item at this location in the dataset
char *get_value_at(cursor_t *cursor,identifier_t *iden){
    char *value=0;

    return value;
}



expression_value_t *eval_token(token_t *token){
    expression_value_t *expr=safe_malloc(sizeof(expression_value_t),1);
    expr->type=0;
    switch(token->type){
        case TOKEN_STRING:  expr->STRING_V=token->value; 
                            expr->type=EVAL_STRING;
                            break;
        case TOKEN_NUMERIC: expr->LONG_V=atol(token->value); 
                            expr->type=EVAL_LONG;
                            break;
        //case TOKEN_HEX:     expr->LONG_V=stoi(&token->value+2, 0, 16);
        //                    expr->type=EVAL_LONG;
        //                    break; 
        //case TOKEN_BINARY:  expr->LONG_V=stoi(&token->value+2, 0, 2);
        //                    expr->type=EVAL_LONG;
        //                    break; 
        case TOKEN_REAL:    expr->FLOAT_V=atof(token->value); 
                            expr->type=EVAL_FLOAT;
                            break;
        case TOKEN_NULL:    expr->type=EVAL_NULL;
                            break; 
    }    
    return expr;
}

expression_value_t *evaluate_expression(cursor_t *cursor,expression_t **expr){
    expression_t *temp_expr=*expr;
    expression_value_t *exprV=0;
    expression_value_t *tempV=0;

    //printf ("in evaluate expression\n");
    int next_operation=0;

    
    while(temp_expr) {

        // compare the expression (eject higher functions problem)
        // if found after the first element.. eject
        // if not... its the first comparitor
        if(exprV && temp_expr->comparison_operator) {
            if(tempV!=exprV) free(tempV);
            *expr=temp_expr;
            return exprV;
        }
                // process token(s)/data point.. 
        switch(temp_expr->mode){
            // identifier (lets just make this a token code)
            case 1: //char *value=get_value_at(cursor,temp_expr->identifier);
                    //type=TOKEN_IDENTIFIER;
                    //exprV=
                    tempV=safe_malloc(sizeof(expression_value_t),1);
                    tempV->type=EVAL_STRING;
                    tempV->STRING_V="BOB";
                    //printf ("identifier\n");
                    break;
                    // litteral
            case 2: //printf ("EVAL->\"%s\"\n",temp_expr->literal->value);
                    tempV=eval_token(temp_expr->literal); 
                    //printf ("DONE\n");
                    break;
            // logical operator
            case 6:     if(tempV!=exprV) free(tempV);
                        *expr=temp_expr;
                        return exprV;
                    
            default: printf ("No clue what this is evaluate expression %d\n",temp_expr->mode);
                     if(exprV) free(exprV);
                     if(tempV) free(tempV);
                    *expr=temp_expr;
                     return 0;
                     break;
        }// end switch 

       // process a prefix + or - 
        if(temp_expr->uinary_operator) {
            //printf("Uinary\n");
            //nothing to do for the "+" operator
            if(temp_expr->uinary_operator==TOKEN_MINUS){
                switch(tempV->type){
                    case TOKEN_STRING:  printf("cannot apply uinary operation to a string");
                                        if(exprV) free(exprV);
                                        if(tempV) free(tempV);
                                        *expr=temp_expr;
                                        return 0;
                    case TOKEN_NULL:    printf("cannot apply uinary operation to a NULL");
                                        if(exprV) free(exprV);
                                        if(tempV) free(tempV);
                                        *expr=temp_expr;
                                        return 0;
                    case TOKEN_NUMERIC: 
                    case TOKEN_HEX:     
                    case TOKEN_BINARY:  
                    case TOKEN_REAL:    tempV->LONG_V*=-1;
                                        break;
                }
            }
        }// end uniary check for expr

        //this is loop+1 and arithmetic is flagged
        //schedule math on the expression
        if(temp_expr->arithmetic_operator) {
            if(exprV->type==EVAL_STRING) {
                printf ("Eval Expression: cannot preform arithmetic on a string");
                if(exprV) free(exprV);
                if(tempV) free(tempV);
                *expr=temp_expr;
                return 0;
            }

            if(exprV->type==EVAL_NULL) {
                printf ("Eval Expression: cannot preform arithmetic on a NULL");
                if(exprV) free(exprV);
                if(tempV) free(tempV);
                *expr=temp_expr;
                return 0;
            }

           // convert matrix.. 
           // if 'x' convert to the greater type
           //  +i|l|f+
           // ---------
           // l+ | |x+
           // f+ | | +


            int t1=exprV->type;
            int t2=tempV->type;
            // convert incompatible types to compatible 
            // ones (prevent loss of precision...)
            if(t1!=t2) {
                if(t1==EVAL_LONG && t2==EVAL_FLOAT)  { 
                    exprV->FLOAT_V=(float)exprV->LONG_V; 
                    exprV->LONG_V=0; 
                    exprV->type=EVAL_FLOAT; 
                    //printf ("converting t1 to FLOAT\n");
                }
                if(t1==EVAL_INT && t2==EVAL_FLOAT)  { 
                    exprV->FLOAT_V=(float)exprV->INT_V; 
                    exprV->INT_V=0; 
                    exprV->type=EVAL_FLOAT; 
                    //printf ("converting t1 to FLOAT\n");
                }

                if(t1==EVAL_INT && t2==EVAL_LONG)  { 
                    exprV->LONG_V=(long)exprV->INT_V; 
                    exprV->INT_V=0; 
                    exprV->type=EVAL_LONG; 
                    //printf ("converting t1 to LONG\n");
                }
            }




            switch(temp_expr->arithmetic_operator){
                case TOKEN_MINUS    :break;
                case TOKEN_PLUS     :break;
                case TOKEN_MULTIPLY :break;
                case TOKEN_DIVIDE   :break;
                case TOKEN_MODULUS  :break;
                default:    printf("Unknown arithmetic operation %d",temp_expr->arithmetic_operator);
                            if(exprV) free(exprV);
                            if(tempV) free(tempV);
                            *expr=temp_expr;
                            return 0;
            }
            

            
            // do the math between the differing types
            switch(exprV->type){
                case EVAL_INT:
                                switch(t2){
                                    case EVAL_INT:   
                                                    switch(temp_expr->arithmetic_operator){
                                                        case TOKEN_MINUS    : exprV->INT_V-=tempV->INT_V; break;
                                                        case TOKEN_PLUS     : exprV->INT_V+=tempV->INT_V; break;
                                                        case TOKEN_MULTIPLY : exprV->INT_V*=tempV->INT_V; break;
                                                        case TOKEN_DIVIDE   : exprV->INT_V/=tempV->INT_V; break; 
                                                        case TOKEN_MODULUS  : exprV->INT_V%=tempV->INT_V; break;
                                                    }
                                                    break;

                                    case EVAL_FLOAT:
                                                    switch(temp_expr->arithmetic_operator){
                                                        case TOKEN_MINUS    : exprV->INT_V-=(int)tempV->FLOAT_V; break;
                                                        case TOKEN_PLUS     : exprV->INT_V+=(int)tempV->FLOAT_V; break;
                                                        case TOKEN_MULTIPLY : exprV->INT_V*=(int)tempV->FLOAT_V; break;
                                                        case TOKEN_DIVIDE   : exprV->INT_V/=(int)tempV->FLOAT_V; break; 
                                                        case TOKEN_MODULUS  : exprV->INT_V%=(int)tempV->FLOAT_V; break;
                                                    }
                                                    break;

                                    case EVAL_LONG:
                                                    switch(temp_expr->arithmetic_operator){
                                                        case TOKEN_MINUS    : exprV->INT_V-=(int)tempV->LONG_V; break;
                                                        case TOKEN_PLUS     : exprV->INT_V+=(int)tempV->LONG_V; break;
                                                        case TOKEN_MULTIPLY : exprV->INT_V*=(int)tempV->LONG_V; break;
                                                        case TOKEN_DIVIDE   : exprV->INT_V/=(int)tempV->LONG_V; break; 
                                                        case TOKEN_MODULUS  : exprV->INT_V%=(int)tempV->LONG_V; break;
                                                    }
                                                    break;
                                    default :
                                                printf("Error in trype conversion");
                                                if(exprV) free(exprV);
                                                if(tempV) free(tempV);
                                                *expr=temp_expr;
                                                return 0;
                                }// internal switch
                            break;

            case EVAL_FLOAT:
                            switch(t2){
                                case EVAL_INT:   
                                                switch(temp_expr->arithmetic_operator){
                                                    case TOKEN_MINUS    : exprV->FLOAT_V-=(float)tempV->INT_V; break;
                                                    case TOKEN_PLUS     : exprV->FLOAT_V+=(float)tempV->INT_V; break;
                                                    case TOKEN_MULTIPLY : exprV->FLOAT_V*=(float)tempV->INT_V; break;
                                                    case TOKEN_DIVIDE   : exprV->FLOAT_V/=(float)tempV->INT_V; break; 
                                                    case TOKEN_MODULUS  : exprV->FLOAT_V=fmod((double)tempV->INT_V,(double)exprV->FLOAT_V); break;
                                                }
                                                break;

                                case EVAL_FLOAT:
                                                switch(temp_expr->arithmetic_operator){
                                                    case TOKEN_MINUS    : exprV->FLOAT_V-=tempV->FLOAT_V; break;
                                                    case TOKEN_PLUS     : exprV->FLOAT_V+=tempV->FLOAT_V; break;
                                                    case TOKEN_MULTIPLY : exprV->FLOAT_V*=tempV->FLOAT_V; break;
                                                    case TOKEN_DIVIDE   : exprV->FLOAT_V/=tempV->FLOAT_V; break; 
                                                    case TOKEN_MODULUS  : exprV->FLOAT_V=fmod((double)tempV->FLOAT_V,(double)exprV->FLOAT_V); break;
                                                }
                                                break;

                                case EVAL_LONG:
                                                switch(temp_expr->arithmetic_operator){
                                                    case TOKEN_MINUS    : exprV->FLOAT_V-=(float)tempV->LONG_V; break;
                                                    case TOKEN_PLUS     : exprV->FLOAT_V+=(float)tempV->LONG_V; break;
                                                    case TOKEN_MULTIPLY : exprV->FLOAT_V*=(float)tempV->LONG_V; break;
                                                    case TOKEN_DIVIDE   : exprV->FLOAT_V/=(float)tempV->LONG_V; break; 
                                                    case TOKEN_MODULUS  : exprV->FLOAT_V=fmod((double)tempV->LONG_V,(double)exprV->FLOAT_V); break;
                                                }
                                                break;
                                default :
                                            printf("Error in trype conversion");
                                            if(exprV) free(exprV);
                                            if(tempV) free(tempV);
                                            *expr=temp_expr;
                                            return 0;
                            }// internal switch
                            break;

            case EVAL_LONG:
                            switch(t2){
                                case EVAL_INT:   
                                                switch(temp_expr->arithmetic_operator){
                                                    case TOKEN_MINUS    : exprV->LONG_V-=(long)tempV->INT_V; break;
                                                    case TOKEN_PLUS     : exprV->LONG_V+=(long)tempV->INT_V; break;
                                                    case TOKEN_MULTIPLY : exprV->LONG_V*=(long)tempV->INT_V; break;
                                                    case TOKEN_DIVIDE   : exprV->LONG_V/=(long)tempV->INT_V; break;
                                                    case TOKEN_MODULUS  : exprV->LONG_V%=(long)tempV->INT_V; break;
                                                }
                                                break;

                                case EVAL_FLOAT:
                                                switch(temp_expr->arithmetic_operator){
                                                    case TOKEN_MINUS    : exprV->LONG_V-=(long)tempV->FLOAT_V; break;
                                                    case TOKEN_PLUS     : exprV->LONG_V+=(long)tempV->FLOAT_V; break;
                                                    case TOKEN_MULTIPLY : exprV->LONG_V*=(long)tempV->FLOAT_V; break;
                                                    case TOKEN_DIVIDE   : exprV->LONG_V/=(long)tempV->FLOAT_V; break;
                                                    case TOKEN_MODULUS  : exprV->LONG_V%=(long)tempV->FLOAT_V; break;
                                                }
                                                break;

                                case EVAL_LONG:
                                                switch(temp_expr->arithmetic_operator){
                                                    case TOKEN_MINUS    : exprV->LONG_V-=tempV->LONG_V; break;
                                                    case TOKEN_PLUS     : exprV->LONG_V+=tempV->LONG_V; break;
                                                    case TOKEN_MULTIPLY : exprV->LONG_V*=tempV->LONG_V; break;
                                                    case TOKEN_DIVIDE   : exprV->LONG_V/=tempV->LONG_V; break;
                                                    case TOKEN_MODULUS  : exprV->LONG_V%=tempV->LONG_V; break;
                                                }
                                                break;
                                default :   printf("Error in trype conversion");
                                            if(exprV) free(exprV);
                                            if(tempV) free(tempV);
                                            *expr=temp_expr;
                                            return 0;
                            }// internal switch
                            break;
                default :   printf("Error in trype conversion");
                            if(exprV) free(exprV);
                            if(tempV) free(tempV);
                            *expr=temp_expr;
                            return 0;
            }// end master outer switch
        }

        if(exprV==0) exprV=tempV;
     

        temp_expr=temp_expr->expression;
        // if this is a first cycle.. assign to root.. 
    }// end while...

    if(tempV && tempV!=exprV) free(tempV);

    //update pointer if successfull
    *expr=temp_expr;
    

    return exprV;
}

int compare_expressions(cursor_t *cursor,expression_t **expr){
    expression_value_t *exprV1=evaluate_expression(cursor,expr);

    //debug_expression_value(exprV1);

    //compare the expression
    expression_t *expr2=*expr;

    if(!expr2) {
        if(exprV1) return 1;
        else return 0;
    }

    if(expr2->comparison_operator) {
        int comparison=expr2->comparison_operator;
        expression_value_t *exprV2=evaluate_expression(cursor,expr);
        //debug_expression_value(exprV2);

        int results=compare_expression_value(exprV1,exprV2,comparison);
        return results;
    } // end if comparitor

    if(exprV1) return 1;
    return 0;
}



int evaluate_expressions(cursor_t *cursor,expression_t *expr){
    expression_t *temp_expr=expr;
    //debug_expr(expr,30);
    // the default evaluation type

    int compare=0;
    int logical_operator=0;
    int bool_value1=0;   //start off false
    int bool_value2=0;
    bool_value1=compare_expressions(cursor,&temp_expr);
    //printf("%d res\n",bool_value1);
    int bool_master=0;
    while(temp_expr) {
        if(!temp_expr) break;
        logical_operator=temp_expr->logical_operator;
        if(logical_operator){
            // advance pointer
            temp_expr=temp_expr->expression;
            bool_value2=compare_expressions(cursor,&temp_expr);
            
            switch(logical_operator) {
                case TOKEN_SHORT_AND :
                case TOKEN_SHORT_OR  :
                case TOKEN_AND       : if(!bool_value1 || !bool_value2) bool_value1=0; break;
                case TOKEN_OR        : if(bool_value1) { 
                                            //printf("ESCAPE\n");
                                            return 1; 
                                        } else bool_value1=bool_value2; break; //any successfull OR is valid
                default:printf("Error Invalid Logical Operator %d",logical_operator);
                            return 0;
            }
            //printf("%d res2\n",bool_value2);
        } else {
            //printf ("not sure\n");
            debug_expr(temp_expr,10);
            break;    
        }// end if logical operator
        //printf("calc bool %d\n",bool_value1);
        // advance pointer

    }

    return bool_value1;
}

// i=1 or 
// i=2 or 
// i=3 or 
// i=4 or 
// i=5 and i=6 or
// i=5 and i=7

int execute_select(cursor_t * cursor,select_t *select){
    /*
    Order of execution...
    1 FROM (including JOIN)
    2 WHERE 
    3 GROUP BY 
    4 HAVING 
    5 SELECT
        5.1 SELECT list
        5.2 DISTINCT
    6 ORDER BY 
    7 TOP / OFFSET-FETCH
    */
    int data_set_count=0;
    data_set_t **data_sets=0;
    if(select->from) ++data_set_count;
    data_set_count+=select->join_length;
    cursor->source_count=data_set_count;

    // for now we will load everything in 1 set of sets
    // phase 2 will be to cunk the data as needed
    // to minimize the memory foot print Such as blocks of 1 MB
    if(data_set_count>0) {
        data_sets=safe_malloc(sizeof(data_set_t),data_set_count);

        if(select->from){
            data_sets[0]=load_file(cursor,select->from);
            if(select->join) {
                for(int i=0;i<select->join_length;i++) {
                    data_sets[i+1]=load_file(cursor,select->join[i].identifier);
                }
            }
        }
    }

    
    // make result data set

    //loop through from and joins
    // only add rows that pass WHERE, then JOIN ON
    int row_count=0;
    
    char ** columns=get_column_list(select->columns,select->column_length);
    data_set_t *results=new_data_set(columns,select->column_length,row_count);
    free_column_list(columns,select->column_length);

    cursor->source =data_sets;
    cursor->results=results;

    // MVP SIMPLE assignment..
    // PHASE 2 LIKE IN % %
    // PHASE 3 SUB QUERY
    
    if(select->where) {
        long row_count_max=0;
        for(int set=0;set<data_set_count;set++){ 
            if(set==0) row_count_max=data_sets[set]->row_length;
            else row_count_max*=data_sets[set]->row_length;
        }

        printf("row_count_max: %ld\n",row_count_max);

        // loop through JOIN
        // (INNER) JOIN: Returns records that have matching values in both tables
        // LEFT (OUTER) JOIN: Returns all records from the left table, and the matched records from the right table
        // RIGHT (OUTER) JOIN: Returns all records from the right table, and the matched records from the left table
        // FULL (OUTER) JOIN: Returns all records when there is a match in either left or right table
        
        long results=return_match(cursor,select,0);
     
     /*
        // set data source
        data_set_t *data_set=cursor->source[set];
        int type=TOKEN_WHERE;
        if(set>0) type=select->join[set-1].type;
        
        for(long i=0;i<data_set->row_length;i++){
            printf("%ld,",i);
    //        data_set[set]->position=i;
            expression_t *expressions=0;

            int results=evaluate_expressions(cursor,expressions);
            results=1;
    

            // -2=skip everything
            // -1 add blank row
            // >-1 add row
            
            //results=evaluate_expressions(cursor,expressions);
            
            switch(type){
                case TOKEN_WHERE:        if(!results) {
                                            data_sets[set]->position=-2;
                                            continue;
                                        }
                                        break;
                case TOKEN_JOIN:        if(!results) {
                                            data_sets[set]->position=-2;
                                            continue;
                                        }
                                        break;
                case TOKEN_LEFT_JOIN:   if(!results) {
                                            data_sets[set]->position=-1;
                                        }
                                        break;
                                break;
            }
        } //end set inner loop (rows)



        }//end set outer loop (source)
*/
    }// end where/join
    
    

    // free data sets
    if(data_set_count>0) {
        //ok we have the data we need. clear out the loaded data sets
        for(int i=0;i<data_set_count;i++) free_data_set(data_sets[i]);
        free(data_sets);
        cursor->source=0;
    }
    
    cursor->results=results;
    
    return 1;
}

long return_match(cursor_t *cursor,select_t *select,int set){
    int type;
    long results=0;
    long length     =cursor->source[set]->row_length;
    long match[length];
    expression_value_t *expr=0;
    if(set==0) {
        type=TOKEN_WHERE;
        expr=select->where;
    }
    else {
        type=select->join[set-1].type;
        expr=select->join[set-1].expression;
    }
    
    data_set_t *data_set=cursor->source[set];

    

    for(long row=0;row<2;row++){
        if(set+1==cursor->source_count) {
            for(int s=0;s<set;s++) {
                printf("%d-",cursor->source[s]->position);
            } 
            printf("%d \n",row);
        }
        cursor->source[set]->position=row;                 //evaluate_expressions(cursor,expr);
        if(set+1<cursor->source_count)
            return_match(cursor,select,set+1);
    }
    

    return results;
}


char ** get_column_list(data_column_t *columns,int length){
    if(length>0){
        char **column_list=(char**)safe_malloc(sizeof(char*),length);
        long index=0;
        data_column_t *temp_data_column=columns;
        while (temp_data_column){
            column_list[index]=strdup(temp_data_column->object);
            temp_data_column=temp_data_column->next;
            ++index;
        }    
        return column_list;
    }
    return 0;
}

range_t * get_line(char *data,long *position,long fsize) {
    if(*position>=fsize) {
        //printf("OUT OF BOUNDS %ld of %ld\n",*position,fsize);
        return 0;
    }


    range_t *range=(range_t*)safe_malloc(sizeof(range_t),1);
    range->end=0;
    range->start=*position;
    for(long pos=*position;pos<fsize;pos++){
        if(data[pos]==LINE_ENDING) {
            range->end=pos;
            break;        
        }
    }
    if(range->end==0) {
        range->end=fsize;
    }
    *position=range->end+1;
    return range;
}

row_t * build_row(char *data,range_t *range,char delimiter){
    // loop through range and split into columns
    row_t *row=(row_t*)safe_malloc(sizeof(row_t),1);
    
    int in_block=0;
    int start_pos=0;
    for(long pos=range->start;pos<range->end;pos++){
        //detect quoted string blocks
        //detect quoted string blocks
        if(start_pos==pos && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=1;
            continue;
        }
        if(in_block==1 && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=0;
            continue;
        }
        if(data[pos]==delimiter) {
            ++row->column_length;
            start_pos=pos+1;
        }
    }//end row splitter

    // adding start column (off by 1)
    // ensures empty lines have 0 columns
    if(range->end-range->start>0) {
        ++row->column_length;
    }
    //printf("%d \n",row->column_length);
    row->columns=(char**)safe_malloc(sizeof(char*),row->column_length+1);
    //scan the row and duplicate the data into the columns
    in_block=0;
    int ordinal=0;
    start_pos=range->start;
    for(long pos=range->start;pos<range->end;pos++){
        //detect quoted string blocks
        if(start_pos==pos && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=1;
            continue;
        }
        if(in_block==1 && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=0;
            continue;
        }

        if(data[pos]==',' || pos+1==range->end) {
            int len=pos-start_pos;
            if(len>=0) {
                char *value=(char*)safe_malloc(len+1,1);
                if(len>0) {
                    memcpy(value,&data[start_pos],len);
                }
                row->columns[ordinal]=value;
            }
            ++ordinal;
            start_pos=pos+1;
        }
    }//end row splitter

    return row;
}

data_set_t * load_file(cursor_t *cursor,identifier_t *table_ident){

    table_def_t *table=get_table_by_identifier(cursor,table_ident);
    
    // does the table ident exist
    if(table) {
        // does not work at all the same wai in pytho
        // maybe i was just totally wrong.. wth?
       // lock_file(table->file);
        
        // read the file into a memory block in 1 chunk
        FILE *f = fopen(table->file, "rb");
        long fsize=0;
        char *data=0;
        if(f) {
            fseek(f, 0, SEEK_END);
            fsize = ftell(f);
            fseek(f, 0, SEEK_SET);

            data = malloc(fsize + 1);
            fread(data, 1, fsize, f);
            fclose(f);
            data[fsize] = 0;
        } else {
            char *err_msg=(char*)safe_malloc(1024,1);
            sprintf(err_msg,"cannot open file '%s'",table->file);
            error(cursor,ERR_FILE_OPEN_ERROR,err_msg);
            return 0;
        }

        //if no data abort
        if(data==0) {
            char *err_msg=(char*)safe_malloc(1024,1);
            sprintf(err_msg,"returned data empty. '%s'",table->file);
            error(cursor,ERR_DATA_FETCH_ERROR,err_msg);
            return 0;
        }
        
        //allocate dataset main container
        data_set_t * data_set=(data_set_t*)safe_malloc(sizeof(data_set_t),1);

        // count rows of data
        long lines=0;
        //long last_line=0;
        for(long i=0;i<fsize;i++){
            if(data[i]==LINE_ENDING) {
                ++lines;
                //last_line=i;
            }
        }
        
        //update data set and allocate row structure
        data_set->row_length=lines;
        data_set->rows=(row_t**)safe_malloc(sizeof(row_t),lines);

        int line=0;
        // quoted
        // strings
        // delimiter
        // array
        long i=0;
        char delimiter=',';
        if(table->column) delimiter=table->column[0];
        long position=1;
        

        
 
        range_t *range=get_line(data,&position,fsize);

        long index=0;
        long max_columns=0;
        while(range){
            //printf("Range %ld-%ld\n",range->start,range->end);

            row_t *row=build_row(data,range,delimiter);
            row->file_row=index;
            if(row->column_length>max_columns) max_columns=row->column_length;
            data_set->rows[index]=row;
            // TAIL 
            free(range);
            range=get_line(data,&position,fsize);
            ++index;
        }
        // free the data that was read        
        free(data);

        // init the column pointer lookup table
        data_set->columns=(char**)safe_malloc(sizeof(char*),max_columns);


        // copy column names for defined columns
        data_column_t * temp_data_column=table->columns;
        long ordinal=0;
        while(temp_data_column){
            data_set->columns[ordinal]=strdup(temp_data_column->object);
            temp_data_column=temp_data_column->next;
            ++ordinal;
        }
        // add default column names for undefined columns with `col_`+ordinal
        while(ordinal<max_columns){
            char *col_name=safe_malloc(sizeof(char),20);
            sprintf(col_name,"col_%ld",ordinal);
            data_set->columns[ordinal]=col_name;
            ++ordinal;
        }
        // update the max number of columns 
        // rows will have whatever they find
        // this refers to mas possible per row
        data_set->column_length=max_columns;
        //debug_dataset(data_set);
        return data_set;
    }
    return 0;
}

int lock_file(char *file){
    struct sockaddr_un sun;
      if(strlen(file)>strlen(sun.sun_path)+1) return 0;
  
    int s = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s < 0) {
        
        return 0;
    }
    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    strcpy(sun.sun_path + 1,file);
    if (bind(s, (struct sockaddr *) &sun, sizeof(sun))) {
        perror("bind");
        return 0;
    }
    return 1;
}


data_set_t * new_data_set(char **columns,int column_count,int row_count){

    //allocate dataset main container
    data_set_t * data_set=(data_set_t*)safe_malloc(sizeof(data_set_t),1);

    //update data set and allocate row structure
    data_set->row_length=row_count;

    
    // init the row pointer lookup table
    if(row_count>0) {
        data_set->rows=(row_t**)safe_malloc(sizeof(row_t),row_count);
    }

    // update the max number of columns 
    data_set->column_length=column_count;

    // init the column pointer lookup table
    if(column_count>0){
        data_set->columns=(char**)safe_malloc(sizeof(char*),column_count);
    
        // copy column names for defined columns
        for(int i=0;i<column_count;i++){
            data_set->columns[i]=strdup(columns[i]);
        }
    }


    return data_set;
}

char * long_2_string(long value){
    const int n = snprintf(NULL, 0, "%lu", value);
    assert(n > 0);
    char * buf=safe_malloc(sizeof(char)*(n+1),1);
    int c = snprintf(buf, n+1, "%lu", value);
    assert(c == n);
    return buf;
}

char * int_2_string(int value){
    const int n = snprintf(NULL, 0, "%d", value);
    assert(n > 0);
    char * buf=safe_malloc(sizeof(char)*(n+1),1);
    int c = snprintf(buf, n+1, "%d", value);
    assert(c == n);
    return buf;
}

char * float_2_string(float value){
    const int n = snprintf(NULL, 0, "%f", value);
    assert(n > 0);
    char *buf=safe_malloc(sizeof(char)*(n+1),1);
    int c = snprintf(buf, n+1, "%f", value);
    assert(c == n);
    return buf;
}

int compare_expression_value(expression_value_t *e1,expression_value_t *e2,int comparison){
    if(e1==0) {
        printf("Error.. comparitor empty\n");
        return 0;
    }
    if(e1->type==EVAL_STRING){
        switch(comparison){
            case TOKEN_IS_NOT_NULL: if(e1->type!=EVAL_NULL) return 1; break;
            case TOKEN_IS_NULL    : if(e1->type==EVAL_NULL) return 1; break;
            case TOKEN_NULL_EQ    : if(e2->type==EVAL_NULL) return 1;  
                                    if(e2->type==EVAL_STRING  && stricmp(e1->STRING_V,e2->STRING_V)==1) return 1;
                                    if(e2->type==EVAL_INT     && stricmp(e1->STRING_V,int_2_string(e2->INT_V))==1) return 1;
                                    if(e2->type==EVAL_LONG    && stricmp(e1->STRING_V,long_2_string(e2->LONG_V))==1) return 1;
                                    if(e2->type==EVAL_FLOAT   && stricmp(e1->STRING_V,float_2_string(e2->FLOAT_V))==1) return 1;
                                    break;
            case TOKEN_LESS_EQ    : if(e2->type==EVAL_STRING  && stricmp(e1->STRING_V,e2->STRING_V)<=1) return 1;
                                    if(e2->type==EVAL_INT     && stricmp(e1->STRING_V,int_2_string(e2->INT_V))<=1) return 1;
                                    if(e2->type==EVAL_LONG    && stricmp(e1->STRING_V,long_2_string(e2->LONG_V))<=1) return 1;
                                    if(e2->type==EVAL_FLOAT   && stricmp(e1->STRING_V,float_2_string(e2->FLOAT_V))<=1) return 1;
                                    break;
            case TOKEN_GREATER_EQ : if(e2->type==EVAL_STRING  && stricmp(e1->STRING_V,e2->STRING_V)>=1) return 1;
                                    if(e2->type==EVAL_INT     && stricmp(e1->STRING_V,int_2_string(e2->INT_V))>=1) return 1;
                                    if(e2->type==EVAL_LONG    && stricmp(e1->STRING_V,long_2_string(e2->LONG_V))>=1) return 1;
                                    if(e2->type==EVAL_FLOAT   && stricmp(e1->STRING_V,float_2_string(e2->FLOAT_V))>=1) return 1;
                                    break;
            case TOKEN_LESS       : if(e2->type==EVAL_STRING  && stricmp(e1->STRING_V,e2->STRING_V)<1) return 1;
                                    if(e2->type==EVAL_INT     && stricmp(e1->STRING_V,int_2_string(e2->INT_V))<1) return 1;
                                    if(e2->type==EVAL_LONG    && stricmp(e1->STRING_V,long_2_string(e2->LONG_V))<1) return 1;
                                    if(e2->type==EVAL_FLOAT   && stricmp(e1->STRING_V,float_2_string(e2->FLOAT_V))<1) return 1;
                                    break;
            case TOKEN_GREATER    : if(e2->type==EVAL_STRING  && stricmp(e1->STRING_V,e2->STRING_V)>1) return 1;
                                    if(e2->type==EVAL_INT     && stricmp(e1->STRING_V,int_2_string(e2->INT_V))>1) return 1;
                                    if(e2->type==EVAL_LONG    && stricmp(e1->STRING_V,long_2_string(e2->LONG_V))>1) return 1;
                                    if(e2->type==EVAL_FLOAT   && stricmp(e1->STRING_V,float_2_string(e2->FLOAT_V))>1) return 1;
                                    break;
            case TOKEN_NOT_EQ     : if(e2->type==EVAL_STRING  && stricmp(e1->STRING_V,e2->STRING_V)!=1) return 1;
                                    if(e2->type==EVAL_INT     && stricmp(e1->STRING_V,int_2_string(e2->INT_V))!=1) return 1;
                                    if(e2->type==EVAL_LONG    && stricmp(e1->STRING_V,long_2_string(e2->LONG_V))!=1) return 1;
                                    if(e2->type==EVAL_FLOAT   && stricmp(e1->STRING_V,float_2_string(e2->FLOAT_V))!=1) return 1;
                                    break;
            case TOKEN_ASSIGNMENT : if(e2->type==EVAL_STRING  && stricmp(e1->STRING_V,e2->STRING_V)==1) return 1;
                                    if(e2->type==EVAL_INT     && stricmp(e1->STRING_V,int_2_string(e2->INT_V))==1) return 1;
                                    if(e2->type==EVAL_LONG    && stricmp(e1->STRING_V,long_2_string(e2->LONG_V))==1) return 1;
                                    if(e2->type==EVAL_FLOAT   && stricmp(e1->STRING_V,float_2_string(e2->FLOAT_V))==1) return 1;
                                    
        }
//        printf( "no match? string");
        return 0;
    }

    if(e1->type==EVAL_NULL){
        switch(comparison){
            case TOKEN_IS_NULL    : if(e1->type==EVAL_NULL) return 1; 
            case TOKEN_NULL_EQ    : if(e2->type==EVAL_NULL) return 1;  
        }
//        printf( "no match? null");
        return 0;
    }
    if(e1->type==EVAL_INT){

        switch(comparison){
            case TOKEN_IS_NOT_NULL: if(e1->type!=EVAL_NULL) return 1; 
            case TOKEN_IS_NULL    : if(e1->type==EVAL_NULL) return 1; 
            case TOKEN_NULL_EQ    : if(e2->type==EVAL_NULL) return 1;  
                                    if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->INT_V))==1) return 1;
                                    if(e2->type==EVAL_INT     && e1->INT_V==e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->INT_V==e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->INT_V==e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_LESS_EQ    : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->INT_V))<=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->INT_V<=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->INT_V<=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->INT_V<=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_GREATER_EQ : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->INT_V))>=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->INT_V>=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->INT_V>=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->INT_V>=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_LESS       : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->INT_V))<1) return 1;
                                    if(e2->type==EVAL_INT     && e1->INT_V<e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->INT_V<e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->INT_V<e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_GREATER    : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->INT_V))>1) return 1;
                                    if(e2->type==EVAL_INT     && e1->INT_V>e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->INT_V>e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->INT_V>e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_NOT_EQ     : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->INT_V))!=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->INT_V!=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->INT_V!=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->INT_V!=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_ASSIGNMENT : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->INT_V))==1) return 1;
                                    if(e2->type==EVAL_INT     && e1->INT_V==e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->INT_V==e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->INT_V==e2->FLOAT_V  ) return 1;
                                    
        }
//        printf( "no match? int");
        return 0;
    }
    if(e1->type==EVAL_FLOAT){

        switch(comparison){
            case TOKEN_IS_NOT_NULL: if(e1->type!=EVAL_NULL) return 1; 
            case TOKEN_IS_NULL    : if(e1->type==EVAL_NULL) return 1; 
            case TOKEN_NULL_EQ    : if(e2->type==EVAL_NULL) return 1;  
                                    if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->FLOAT_V))==1) return 1;
                                    if(e2->type==EVAL_INT     && e1->FLOAT_V==e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->FLOAT_V==e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->FLOAT_V==e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_LESS_EQ    : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->FLOAT_V))<=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->FLOAT_V<=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->FLOAT_V<=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->FLOAT_V<=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_GREATER_EQ : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->FLOAT_V))>=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->FLOAT_V>=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->FLOAT_V>=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->FLOAT_V>=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_LESS       : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->FLOAT_V))<1) return 1;
                                    if(e2->type==EVAL_INT     && e1->FLOAT_V<e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->FLOAT_V<e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->FLOAT_V<e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_GREATER    : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->FLOAT_V))>1) return 1;
                                    if(e2->type==EVAL_INT     && e1->FLOAT_V>e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->FLOAT_V>e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->FLOAT_V>e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_NOT_EQ     : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->FLOAT_V))!=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->FLOAT_V!=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->FLOAT_V!=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->FLOAT_V!=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_ASSIGNMENT : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->FLOAT_V))==1) return 1;
                                    if(e2->type==EVAL_INT     && e1->FLOAT_V==e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->FLOAT_V==e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->FLOAT_V==e2->FLOAT_V  ) return 1;
                                    
        }
//        printf( "no match? float");
        return 0;
    }
    if(e1->type==EVAL_LONG){

        switch(comparison){
            case TOKEN_IS_NOT_NULL: if(e1->type!=EVAL_NULL) return 1; 
            case TOKEN_IS_NULL    : if(e1->type==EVAL_NULL) return 1; 
            case TOKEN_NULL_EQ    : if(e2->type==EVAL_NULL) return 1;  
                                    if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->LONG_V))==1) return 1;
                                    if(e2->type==EVAL_INT     && e1->LONG_V==e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->LONG_V==e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->LONG_V==e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_LESS_EQ    : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->LONG_V))<=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->LONG_V<=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->LONG_V<=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->LONG_V<=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_GREATER_EQ : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->LONG_V))>=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->LONG_V>=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->LONG_V>=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->LONG_V>=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_LESS       : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->LONG_V))<1) return 1;
                                    if(e2->type==EVAL_INT     && e1->LONG_V<e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->LONG_V<e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->LONG_V<e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_GREATER    : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->LONG_V))>1) return 1;
                                    if(e2->type==EVAL_INT     && e1->LONG_V>e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->LONG_V>e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->LONG_V>e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_NOT_EQ     : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->LONG_V))!=1) return 1;
                                    if(e2->type==EVAL_INT     && e1->LONG_V!=e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->LONG_V!=e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->LONG_V!=e2->FLOAT_V  ) return 1;
                                    break;
            case TOKEN_ASSIGNMENT : if(e2->type==EVAL_STRING  && stricmp(e2->STRING_V,int_2_string(e1->LONG_V))==1) return 1;
                                    if(e2->type==EVAL_INT     && e1->LONG_V==e2->INT_V    ) return 1;
                                    if(e2->type==EVAL_LONG    && e1->LONG_V==e2->LONG_V   ) return 1;
                                    if(e2->type==EVAL_FLOAT   && e1->LONG_V==e2->FLOAT_V  ) return 1;
                                    
        }
//        printf( "no match? long");
        return 0;
    }
    printf("NO CLUE WHATS UP WITH THIS TYPE COMPARISON %d\n",comparison);

    return 0;

}