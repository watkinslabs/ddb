// message output
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"

#define DEBUG 1
//#define PARSE_ENTRANCE 1


void gabble(char *source,char *msg){
#if defined(DEBUG)

    char *type="ERR";
    printf(" %s %s: %s\n",type,source,msg);
#endif
}

void gobble(char *source,char *msg){
#if defined(DEBUG)
    char *type="INFO";
    printf(" %s %s: %s\n",type,source,msg);
#endif
}

void goop(int depth,char *source,char *msg){
 #if defined(DEBUG)
    for (int i=0;i<depth;i++) printf(" ");
    printf("%s: %s\n",source,msg);
 #endif
}   

char * vomit(int err_no){
    char *msg;
    switch(err_no){
        case ERR_UNTERMINATED_COMMENT_BLOCK      : msg="unterminated comment block"; break;
        case ERR_MEMORY_ALLOCATION_ERR           : msg="memory allocation error."; break;
        case ERR_FILE_OPEN_ERROR                 : msg="file open error"; break;
        case ERR_EMPTY_QUERY_STRING              : msg="empty query string"; break;
        case ERR_EMPTY_STRING                    : msg="empty string"; break;
        case ERR_STRING_DUPLICATION_ERROR        : msg="string duplication error"; break;
        case ERR_OUT_OF_BOUNDS                   : msg="index out of bounds"; break;
        case ERR_UNTERMINATED_STRING             : msg="unterminated string"; break;
        case ERR_MALFORMED_HEX_TOKEN             : msg="malformed hex token"; break;
        case ERR_MALFORMED_BINARY_TOKEN          : msg="malformed binary token"; break;
        case ERR_UNKNOWN_SQL                     : msg="unknown sql"; break;
        case ERR_TOKENS_FULL                     : msg="tokens full"; break;   
        case ERR_TOKENS_OUT_OF_BOUNDS            : msg="tokens out of bounds"; break;           
        case ERR_TOKENS_EMPTY                    : msg="tokens empty"; break;   
        case ERR_INVALID_SELECT_EXPR_ALIAS       : msg="select expression has an missing or invalid alias"; break;   
        case ERR_UNTERMINATED_LINE_COMENT        : msg="unterminated line comment"; break;
        case ERR_UNTERMINATED_BLOCK_COMENT       : msg="unterminated block comment"; break;
        case ERR_TOKEN_TARGET_NULL               : msg="token target is null"; break;
        case ERR_INVALID_JOIN_ALIAS              : msg="invalid join alias"; break;
        case ERR_JOIN_WITHOUT_ON                 : msg="join without on clause"; break;
        case ERR_JOIN_WITHOUT_EXPR               : msg="join missing conditional expresion"; break;
        case ERR_INVALID_JOIN_IDENTITY           : msg="join identity missing "; break;
        case ERR_NO_TABLE_SELECTED               : msg="No table selected"; break;
        case ERR_TABLE_ALREADY_EXISTS            : msg="Table already exists"; break;
        case ERR_FILE_NOT_FOUND                  : msg="File not found"; break;
        case ERR_FILE_READ_PERMISSION            : msg="File Read permission"; break;
        case ERR_FILE_WRITE_PERMISSION           : msg="File write permission"; break;
        case ERR_TABLE_HAS_NO_COLUMNS            : msg="table has no columns"; break;
        case ERR_INVALID_COLUMN_NAME             : msg="invalid column name"; break;
        case ERR_AMBIGUOUS_COLUMN_NAME           : msg="ambiguous column name"; break;
        case ERR_AMBIGUOUS_JOIN                  : msg="ambiguous join"; break;
        case ERR_AMBIGUOUS_COLUMN_IN_SELECT_LIST : msg="ambiguous column in select list"; break;
        case ERR_SELECT_LIST_UNKNOWN_TOKEN       : msg="unknown token in select list"; break;
        case ERR_MISSING_COLUMNS                 : msg="missing columns in select"; break;
        case ERR_INVALID_FROM_TABLE              : msg="invalid 'FROM' table"; break;
        case ERR_INVALID_JOIN_TABLE              : msg="invalid 'JOIN' table"; break;
        case ERR_INVALID_DATABASE                : msg="invalid database"; break;
        case ERR_COLUMN_NOT_FOUND                : msg="column not found in source table"; break;
        case ERR_INVALID_QUALIFIER               : msg="invalid qualifier"; break;
        case ERR_DUPLICATE_GROUP_BY_COLUMN       : msg="duplicate column in group by"; break;
        case ERR_LIMIT_START_NEGATIVE            : msg="limit start negative"; break;
        case ERR_LIMIT_LENGTH_NEGATIVE           : msg="limit length negative"; break;
        case ERR_LOCKING_ERROR                   : msg="could not achieve a lock on file"; break;
        case ERR_DATA_FETCH_ERROR                : msg="fetching data from file failed"; break;
        case ERR_EXPRESSION_MALFORMED            : msg="expression is malformed"; break;

        default: msg="NO CLUE";
    }
    //printf("%d ",err_no);
    //gabble("SYS",msg);
    return msg;
}

void ghost(int err_no){
    vomit(err_no);
    exit(err_no);
}

char *string_duplicate(const char *str){
    if (str==0) return 0;
    char *new_str = strdup(str);
    if(new_str == NULL)
        ghost(ERR_STRING_DUPLICATION_ERROR);
    return new_str;
}


char *sub_str_cpy(char *data,int start,int length){
    int string_len=strlen(data);
    if(start<0 || start >string_len) {
        ghost(ERR_OUT_OF_BOUNDS);
    }
    if(string_len<length) {
        ghost(ERR_OUT_OF_BOUNDS);
    }
    if( length<1) {
        //exit(EMPTY_STRING);
        return "?";
    }
    char *buffer=calloc(1,length+1);
    
    if (buffer==NULL){
        ghost(ERR_MEMORY_ALLOCATION_ERR);
    }

    memcpy(buffer,&data[start],length);

    return buffer;
}

void error(cursor_t *cursor,int ERR_NUM,char *message){
    if(cursor->error_message) free(cursor->error_message);
    cursor->error=ERR_NUM;
    cursor->error_message=message;

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
    char *pad=" ";
    //if(depth>0) pad=safe_malloc(depth+1,1);

    //for(int i=0;i<depth;i++) pad[i]=' ';

    //printf("%s- expr: ",pad);
    if(expr->mode)        printf("%s  mode:   %d ,",pad,expr->mode);
    if(expr->list)        printf("%s  list:   %d ,",pad,expr->list);
    if(expr->not)         printf("%s  not:    %d ,",pad,expr->not);
    if(expr->not_in)      printf("%s  not_in: %d ,",pad,expr->not_in);
    if(expr->in)          printf("%s  in:     %d \n",pad,expr->in);
    if(expr->direction)   printf("%s  direction:  %s ,\n",pad,token_type(expr->direction));


    if(expr->assignment_operator)  printf(" - assignment operator :%s\n",expr->assignment_operator);
    if(expr->comparison_operator)  printf(" - comparison operator :%s\n",expr->comparison_operator);
    if(expr->arithmetic_operator)  printf(" - arithmetic operator :%s\n",expr->arithmetic_operator);
    if(expr->logical_operator   )  printf(" - logical operator    :%s\n",expr->logical_operator);
    if(expr->uinary_operator    )  printf(" - uinary operator     :%s\n",expr->uinary_operator);

    if(expr->identifier){
        printf("%s Identifier: %s.%s\n",pad,expr->identifier->qualifier,expr->identifier->source);
    }
    if(expr->literal) {
        printf("%s Litteral: [%s] '%s'\n",pad,token_type(expr->literal->type),expr->literal->value);
    }

    //if(depth>0) free(pad);
    if(expr->expression) debug_expr(expr->expression,depth+1);
}

/* Function: debug_identifier
 * -----------------------
 * visibly print the identifier_t data structure
 * 
 * returns: nothing
 */
void debug_identifier(identifier_t *identifier) {
    printf (" - identifier: %s.%s\n",identifier->qualifier,identifier->source);
}

/* Function: debug_create_table
 * -----------------------
 * visibly print the table_def data structure
 * 
 * returns: nothing
 */
void debug_create_table(table_def_t *table) {
    if(table==0) {
        printf ("Cant debug create table. Null\n");
    } 
    printf (" -- CREATE_TABLE DEBUG -------------\n");
    data_column_t *temp_ptr=table->columns;
    debug_identifier(table->identifier);
    
    while(temp_ptr){
        // TODO column value identifier string etc...
        if(temp_ptr->alias) {
            printf(" - Column Alias: %s\n",temp_ptr->alias);
        }
        printf(" - Column Type: %s\n",token_type(temp_ptr->type));
        if(temp_ptr->type==TOKEN_IDENTIFIER) {
            debug_identifier((identifier_t *)temp_ptr->object);
        } else {
            printf(" - Column Value: %s\n",(char *)temp_ptr->object);
        }
        temp_ptr=temp_ptr->next;
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

/* Function: debug_cursor
 * -----------------------
 * visibly print the cursor data structure
 * 
 * returns: nothing
 */
void debug_cursor(cursor_t *cursor){

    printf("\n# Cursor\n");

    printf("- Database: %s\n", cursor->active_database );
    printf("- Created : %lld.%.9ld\n", (long long)cursor->created.tv_sec, cursor->created.tv_nsec);
    printf("- Ended   : %lld.%.9ld\n", (long long)cursor->ended.tv_sec, cursor->ended.tv_nsec);
    
    printf("- Ellapsed: %ld.%09ld\n", (long)(cursor->ended.tv_sec - cursor->created.tv_sec),
        cursor->ended.tv_nsec - cursor->created.tv_nsec);
        
    int                 data_length;
    if(cursor->status==EXIT_SUCCESS){
        printf("- Status  : SUCCESS\n");
    }
    if(cursor->status==EXIT_FAILURE) {
        printf("- Status  : FAILURE\n");
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
        printf("- POS: %d\n",cursor->parse_position);
    }

}

void debug_dataset(data_set_t *data){
    debug_header("dataset");
    printf(" - column length: %ld\n",data->column_length);
    printf(" - row_length: %ld\n",data->row_length);

    for(long i=0;i<data->row_length;i++){
        row_t *temp_row=data->rows[i];
        for(long b=0;b<temp_row->column_length;b++){
            if(b!=0) printf(",");
            if(temp_row->columns[b]) {
                printf("%s",temp_row->columns[b]);
            } else {
                printf ("(null)");
            }
        }
        printf("\n");
    }

}


void debug_alias(char *alias) {
    printf ("   - alias: %s\n",alias);
}

void debug_header(char *title) {
    printf (" # %s\n",title);
}

void debug_sub_header(char *title) {
    printf (" ## %s\n",title);
}

void debug_tuple(char *key,char* value){
    printf (" - %s: %s\n",key,value);
}
void debug_value(char* value){
    printf (" - %s\n",value);
}
void debug_ordinal(int ordinal) {
    printf ("   - ordinal: %d\n",ordinal);
}

/* Function: debug_select
 * -----------------------
 * visibly print the select data structure
 * 
 * returns: nothing. All output is via stdio
 */
void debug_select(select_t *select){
    // DEBUGGING INFORMATION

    if(select==0) return;
    debug_header("Select");
    if (select->distinct) debug_sub_header("HAS DISTINCT");
    if (select->columns){
        data_column_t * next=select->columns;
        
        while(next){
            if(next->object==0) debug_sub_header("Missing object in datacolumn");
            else 
            switch(next->type){

                case TOKEN_STRING:
                case TOKEN_NUMERIC:
                case TOKEN_HEX:
                case TOKEN_BINARY:
                case TOKEN_REAL:
                case TOKEN_NULL: 
                
                 printf(" - %s: ",  token_type(next->type));
                 printf(" %s \n", (char*)next->object);
                            debug_alias(next->alias);
                            debug_ordinal(next->ordinal);
                                  break;
                case TOKEN_IDENTIFIER: debug_identifier((identifier_t *)next->object); 
                                       debug_alias(next->alias);
                                       debug_ordinal(next->ordinal);
                                    break;
                default:   debug_value(token_type(next->type));
                            break;
            }//end switch
            next=next->next;
        }//end while
    } else {
        debug_sub_header("NO COLUMNS");
    }
   

     if (select->from) {
        debug_sub_header("FROM");
        debug_identifier(select->from);
        if(select->alias) debug_alias(select->alias);
        
    }

    if (select->join) {
        debug_sub_header("JOIN");// %d\n",select->join_length);
        for(int i=0;i<select->join_length;i++){
            if(select->join[i].identifier) {
                int printed=0;
                if(select->join[i].identifier->qualifier) {
                    printf("%s.",select->join[i].identifier->qualifier);
                    printed=1;
                }
                if(select->join[i].identifier->source) {
                    printf("%s ",select->join[i].identifier->source);
                    printed=1;
                }
                if(select->join[i].alias) {
                    printf("ALIAS: %s",select->join[i].alias);
                    printed=1;
                }
                if(printed==1){
                    printf("\n");
                }
            }
            debug_expr(select->join[i].expression,0);
        }
        
    }
    if(select->where) {
        printf(" ---WHERE---\n");
        debug_expr(select->where,0);
    }
    if(select->group) {
        printf(" ---GROUP---\n");
        debug_expr(select->group,0);
    }
    if(select->order) {
        printf(" ---ORDER---\n");
        debug_expr(select->order,0);
    }


    if (select->has_limit_start) printf("LIMIT_START:   %d\n",select->limit_start);
    if (select->has_limit_length) printf("LIMIT_LENGTH : %d\n",select->limit_length);
}

/* Function: debug_use
 * -----------------------
 * visibly print the use_t data structure
 * 
 * returns: nothing. All output is via stdio
 */
void debug_use(use_t *use){
    // DEBUGGING INFORMATION

    if(use==0) return;
    debug_header("Use");
    if (use->database) printf(" - database: %s\n",use->database);

}