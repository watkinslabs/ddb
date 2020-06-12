#include "tokens.h"
#include "structure.h"

#if !defined(_SELECT_EXPR_H_)
    #define _SELECT_EXPR_H_ 1


    // helpers
    token_t       * token_at                 (token_array_t *tokens,int  index);
    token_t       * duplicate_token          (token_t *tokens);
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
    // debuggers
    void            select_print             (select_t *select);
    // in flight...
    expression_t  * process_column_list      (token_array_t *tokens,int *index);
    table_def_t   * process_create_table     (token_array_t *tokens,int *start);
    expression_t  * process_column_list      (token_array_t *tokens,int *index);
    int             free_data_columns        (data_column_t *columns);
    int             validate_select          (cursor_t *cursor,select_t *select);
    int             fixup_create_table       (cursor_t *cursor,table_def_t *table);
    int             compare_identifiers      (identifier_t *source,identifier_t *dest);
    int             validate_create_table    (cursor_t * cursor,table_def_t *table);
    cursor_t      * init_cursor              ();
    char          * get_current_database     (cursor_t *cursor);
    void            set_error                (cursor_t *cursor,int error_no,char *msg);


    
    void select_debug();
#endif  