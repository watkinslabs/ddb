#include "structure.h"

#if !defined(_FREE_H_)
    #define _FREE_H_ 1
    
    int free_data_columns   (data_column_t *columns);
    int free_data_set       (data_set_t *data_set);
    int free_column_list    (char ** columns,int length);
    int free_select         (select_t *select);
    int free_string         (char *data);
    int free_expression     (expression_t *expr);
    int free_ident          (identifier_t *ident);
    int free_litteral       (token_t *token);
    int free_table_def      (table_def_t *table_def);
    int free_cursor         (cursor_t *cursor);
    int free_use            (use_t *use);
    int free_token          (token_t *token);

#endif