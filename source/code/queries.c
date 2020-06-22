#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"
#include <time.h>

#define EXPRESSION_GROUP_BY 3
#define EXPRESSION_ORDER_BY 4
#define EXPRESSION_COLUMN   5

#define DEFAULT_DATABASE_NAME "this"

char *get_current_database(cursor_t *cursor){
    // always set.. defaults to "information_schema"
    if (cursor->active_database==0) return strdup(DEFAULT_DATABASE_NAME);
    return strdup(cursor->active_database);
}

void set_error(cursor_t *cursor,int error_no,char *msg){
    if(cursor->error_message) {
        free(cursor->error_message);
    }
    cursor->error=error_no;
    cursor->error_message=msg;
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
data_column_t *add_data_column(data_column_t *column,unsigned int type,void *item,char *alias,int ordinal){
    data_column_t *new_column=safe_malloc(sizeof(data_column_t),1);
    new_column->alias=alias;
    new_column->ordinal=ordinal;
    new_column->object=item;
    new_column->type=type;

    if(column==0) {
        column=new_column;
    } else {
        if(column->next==0){
            column->next=new_column;
            column->next_tail=new_column;
            return column;
        } else {
            column->next_tail->next=new_column;
            column->next_tail=new_column;
        }
    }
    return column;
}

table_def_t *get_table_by_identifier(cursor_t *cursor,identifier_t *ident) {
    table_def_t *tmp_table=cursor->tables;
    while (tmp_table) {
        printf(" - GET TABLE \n");
        if(compare_identifiers(ident,tmp_table->identifier)){
            return tmp_table;
        }
        tmp_table=tmp_table->next;
    }
    return 0;
}

cursor_t * init_cursor(){
    cursor_t * cursor=safe_malloc(sizeof(cursor_t),1);
    cursor->data_length=0;
    cursor->parse_position=0;
    cursor->active_database=get_current_database(cursor);
    clock_gettime(CLOCK_REALTIME,&cursor->created);
    return cursor;
}



// TODO create dable validate table

 //Expression to data_column_t