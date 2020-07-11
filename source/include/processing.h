
#if !defined(_PROCESING_H_)
    #define _PROCESING_H_ 1
    #include "structure.h"
    
    char          * process_alias            (cursor_t *cursor,token_array_t *tokens,int *index);
    identifier_t  * process_identifier       (cursor_t *cursor,token_array_t *tokens,int *index);
    token_t       * process_litteral         (cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_simple_expr      (cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_bit_expr         (cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_expr_list        (cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_predicate        (cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_boolean_primary  (cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_expression       (cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_group_column_list(cursor_t *cursor,token_array_t *tokens,int *index);
    expression_t  * process_order_column_list(cursor_t *cursor,token_array_t *tokens,int *index);
    data_column_t * process_select_list      (cursor_t *cursor,token_array_t *tokens,int *index);
    data_column_t * process_column_list      (cursor_t *cursor,token_array_t *tokens,int *index);
#endif