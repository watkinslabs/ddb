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
        case ERR_UNTERMINATED_COMMENT_BLOCK  : msg="unterminated comment block"; break;
        case ERR_MEMORY_ALLOCATION_ERR       : msg="memory allocation error."; break;
        case ERR_FILE_OPEN_ERROR             : msg="file open error"; break;
        case ERR_EMPTY_QUERY_STRING          : msg="empty query string"; break;
        case ERR_EMPTY_STRING                : msg="empty string"; break;
        case ERR_STRING_DUPLICATION_ERROR    : msg="string duplication error"; break;
        case ERR_OUT_OF_BOUNDS               : msg="index out of bounds"; break;
        case ERR_UNTERMINATED_STRING         : msg="unterminated string"; break;
        case ERR_MALFORMED_HEX_TOKEN         : msg="malformed hex token"; break;
        case ERR_MALFORMED_BINARY_TOKEN      : msg="malformed binary token"; break;
        case ERR_UNKNOWN_SQL                 : msg="unknown sql"; break;
        case ERR_TOKENS_FULL                 : msg="tokens full"; break;   
        case ERR_TOKENS_OUT_OF_BOUNDS        : msg="tokens out of bounds"; break;           
        case ERR_TOKENS_EMPTY                : msg="tokens empty"; break;   
        case ERR_INVALID_SELECT_EXPR_ALIAS   : msg="select expression has an missing or invalid alias"; break;   
        case ERR_UNTERMINATED_LINE_COMENT    : msg="unterminated line comment"; break;
        case ERR_UNTERMINATED_BLOCK_COMENT   : msg="unterminated block comment"; break;
        case ERR_TOKEN_TARGET_NULL           : msg="token target is null"; break;
        case ERR_INVALID_JOIN_ALIAS          : msg="invalid join alias"; break;
        case ERR_JOIN_WITHOUT_ON             : msg="join without on clause"; break;
        case ERR_JOIN_WITHOUT_EXPR           : msg="join missing conditional expresion"; break;
        case ERR_INVALID_JOIN_IDENTITY       : msg="join identity missing "; break;
        case ERR_NO_TABLE_SELECTED           : msg="No table selected"; break;
        case ERR_TABLE_ALREADY_EXISTS        : msg="Table already exists"; break;
        case ERR_FILE_NOT_FOUND              : msg="File not found"; break;
        case ERR_FILE_READ_PERMISSION        : msg="File Read permission"; break;
        case ERR_FILE_WRITE_PERMISSION       : msg="File write permission"; break;
        case ERR_TABLE_HAS_NO_COLUMNS        : msg="table has no columns"; break;
        case ERR_INVALID_COLUMN_NAME         : msg="invalid column name"; break;
        case ERR_AMBIGUOUS_COLUMN_NAME       : msg="ambiguous column name"; break;
        
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
    if(expr->direction)   printf("%s  direction:  %s ,",pad,token_type(expr->direction));
    if(expr->negative)    printf("%s  negative:   %d ,",pad,expr->negative);
    if(expr->positive)    printf("%s  positive:   %d ,",pad,expr->positive);
    if(expr->comparitor)  printf("%s  comparitor: %s ,",pad,token_type(expr->comparitor));
    if(expr->operator)    printf("%s  operator:   %s \n",pad,token_type(expr->operator));
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
    printf ("  Identifier: %s.%s\n",identifier->qualifier,identifier->source);

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

/* Function: debug_cursor
 * -----------------------
 * visibly print the cursor data structure
 * 
 * returns: nothing
 */
void debug_cursor(cursor_t *cursor){

    printf("\n# Cursor\n");

    printf("- Active database: %s\n", get_current_database(cursor) );
    printf("- Created: %lld.%.9ld", (long long)cursor->created.tv_sec, cursor->created.tv_nsec);
    printf("- Ended:%lld.%.9ld", (long long)cursor->ended.tv_sec, cursor->ended.tv_nsec);

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

/* Function: debug_select
 * -----------------------
 * visibly print the select data structure
 * 
 * returns: nothing. All output is via stdio
 */
void debug_select(select_t *select){
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

