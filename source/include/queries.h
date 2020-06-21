#include "tokens.h"
#include "structure.h"

#if !defined(_QUERIES_EXPR_H_)
    #define _QUERIES_EXPR_H_ 1
    
    char           * get_current_database     (cursor_t *cursor);
    void             set_error                (cursor_t *cursor,int error_no,char *msg);
    int              compare_identifiers      (identifier_t *source,identifier_t *dest);
    int              compare_literals         (token_t *source,token_t *dest);
    token_t        * duplicate_token          (token_t *src);
    identifier_t   * duplicate_identifier     (identifier_t *ident);
    data_column_t  * duplicate_columns        (data_column_t *columns);
    table_def_t    * duplicate_table          (table_def_t *table);
    token_t        * token_at                 (token_array_t *tokens,int index);
    char           * copy_token_value_at      (token_array_t *tokens,int index);
    int              add_expr                 (expression_t *expression,expression_t *item);
    char           * process_alias            (token_array_t *tokens,int *index);
    identifier_t   * process_identifier       (token_array_t *tokens,int *index);
    token_t        * process_litteral         (token_array_t *tokens,int *index);
    expression_t   * process_simple_expr      (token_array_t *tokens,int *index);
    expression_t   * process_bit_expr         (token_array_t *tokens,int *index);
    expression_t   * process_expr_list        (token_array_t *tokens,int *index);
    expression_t   * process_predicate        (token_array_t *tokens,int *index);
    expression_t   * process_boolean_primary  (token_array_t *tokens,int *index);
    expression_t   * process_expression       (token_array_t *tokens,int *index);
    expression_t   * process_group_column_list(token_array_t *tokens,int *index);
    expression_t   * process_order_column_list(token_array_t *tokens,int *index);
    data_column_t  * add_data_column          (data_column_t *column,unsigned int type,void *item,char *alias,int ordinal);
    data_column_t  * process_select_list      (token_array_t *tokens,int *index);
    select_t       * process_select           (token_array_t *tokens,int *start);
    data_column_t  * process_column_list      (token_array_t *tokens,int *index);
    table_def_t    * process_create_table     (token_array_t *tokens,int *start);
    use_t          * process_use              (token_array_t *tokens,int *start);
    int              validate_select          (cursor_t * cursor,select_t *select);
    table_def_t    * get_table_by_identifier  (cursor_t *cursor,identifier_t *ident) ;
    data_column_t  * match_data_column        (data_column_t *data,char *name,int ignore_ordinal) ;
    int              validate_create_table    (cursor_t * cursor,table_def_t *table);
    cursor_t       * init_cursor              ();
    int              validate_use             (cursor_t *cursor,use_t *use);

#endif  