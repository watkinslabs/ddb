
#if !defined(_DUP_H_)
    #define _DUP_H_ 1
    
    #include "structure.h"
    token_t        * duplicate_token            (token_t *src);
    identifier_t   * duplicate_identifier       (identifier_t *ident);
    char          ** duplicate_data_set_columns (char **columns,long length);
    row_t          * duplicate_dataset_row      (row_t *row);
    data_set_t     * duplicate_data_set         (data_set_t *data_set);
    data_column_t  * duplicate_columns          (data_column_t *columns);
    table_def_t    * duplicate_table            (table_def_t *table);
    cursor_t       * duplicate_cursor           (cursor_t *cursor);
#endif