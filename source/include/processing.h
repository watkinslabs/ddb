#include "structure.h"

#if !defined(_PROCESING_H_)
    #define _PROCESING_H_ 1
    char          * process_alias            (token_array_t *tokens,int *index);
    identifier_t  * process_identifier       (token_array_t *tokens,int *index);
    token_t       * process_litteral         (token_array_t *tokens,int *index);
    expression_t  * process_simple_expr      (token_array_t *tokens,int *index);
    expression_t  * process_bit_expr         (token_array_t *tokens,int *index);
    expression_t  * process_expr_list        (token_array_t *tokens,int *index);
    expression_t  * process_predicate        (token_array_t *tokens,int *index);
    expression_t  * process_boolean_primary  (token_array_t *tokens,int *index);
    expression_t  * process_expression       (token_array_t *tokens,int *index);
    expression_t  * process_group_column_list(token_array_t *tokens,int *index);
    expression_t  * process_order_column_list(token_array_t *tokens,int *index);
    data_column_t * process_select_list      (token_array_t *tokens,int *index);
    data_column_t * process_column_list      (token_array_t *tokens,int *index);
#endif