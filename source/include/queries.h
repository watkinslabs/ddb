#include "tokens.h"

#if !defined(_SELECT_EXPR_H_)
    #define _SELECT_EXPR_H_ 1


    // helpers
    token_t       * token_at                 (token_array_t *tokens,int  index);
    token_t       * duplicate_token          (token_array_t *tokens,int  index);
    char          * copy_token_value_at      (token_array_t *tokens,int  index);
    int             add_expr                 (expression_t *expression,expression_t *item);
    // workers
    char          * process_alias            (token_array_t *tokens,int *index);
    token_t       * process_litteral         (token_array_t *tokens,int *index);
    expression_t  * process_simple_expr      (token_array_t *tokens,int *index);
    expression_t  * process_bit_expr         (token_array_t *tokens,int *index);
    expression_t  * process_expr_list        (token_array_t *tokens,int *index);
    expression_t  * process_predicate        (token_array_t *tokens,int *index);
    expression_t  * process_boolean_primary  (token_array_t *tokens,int *index);
    expression_t  * process_expression       (token_array_t *tokens,int *index);
    expression_t  * process_group_column_list(token_array_t *tokens,int *index);
    expression_t  * process_order_column_list(token_array_t *tokens,int *index);
    select_t      * process_select           (token_array_t *tokens,int *start);
    // cleaners
    int             free_select              (select_t *select) ;
    int             free_string              (char *data);
    int             free_expression          (expression_t *expr);
    int             free_ident               (identifier_t *ident);
    int             free_litteral            (token_t *token);
    int             free_table_def           (table_def_t *table);
    // debuggers
    void            select_print             (select_t *select);
    void            debug_expr               (expression_t *expr,int depth);
    // in flight...
    expression_t  * process_column_list      (token_array_t *tokens,int *index);
    table_def_t   * process_create_table     (token_array_t *tokens,int *start);
    expression_t  * process_column_list      (token_array_t *tokens,int *index);
    int             free_table_def           (table_def_t *table_def);
    void            debug_identifier         (identifier_t *identifier);
    void            debug_create_table       (table_def_t *table);
    int             free_data_columns        (data_column_t *columns);
    int             free_cursor              (cursor_t *cursor);
    int             validate_select          (select_t *select);
    int             fixup_create_table       (cursor_t *cursor,table_def_t *table);
    int             compare_identifiers      (identifier_t *source,identifier_t *dest);
    int             validate_create_table    (cursor_t * cursor,table_def_t *table);
    cursor_t      * init_cursor              ();
    void            debug_cursor             (cursor_t *cursor);
    char          * get_current_database     (cursor_t *cursor);
    void            set_error(cursor_t *cursor,int error_no,char *msg);


    
    void select_debug();
#endif  