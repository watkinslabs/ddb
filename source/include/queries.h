#include "structure.h"

#if !defined(_QUERIES_EXPR_H_)
    #define _QUERIES_EXPR_H_ 1
    
    #define EXPRESSION_GROUP_BY 3
    #define EXPRESSION_ORDER_BY 4
    #define EXPRESSION_COLUMN   5
    #define DEFAULT_DATABASE_NAME "this"

    char            * get_current_database     (cursor_t *cursor);
    void              set_error                (cursor_t *cursor,int error_no,char *msg);
    token_t         * token_at                 (token_array_t *tokens,int index);
    char            * copy_token_value_at      (token_array_t *tokens,int index);
    int               add_expr                 (expression_t *expression,expression_t *item);
    data_column_t   * add_data_column          (data_column_t *column,unsigned int type,void *item,char *alias,int ordinal);
    table_def_t     * get_table_by_identifier  (cursor_t *cursor,identifier_t *ident);
    cursor_t        * init_cursor              ();
#endif  