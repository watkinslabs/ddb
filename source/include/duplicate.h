#include "structure.h"

#if !defined(_DUP_H_)
    #define _DUP_H_ 1
    token_t       * duplicate_token     (token_t *src);
    identifier_t  * duplicate_identifier(identifier_t *ident);
    data_column_t * duplicate_columns   (data_column_t *columns);
    table_def_t   * duplicate_table     (table_def_t *table);
    cursor_t      * duplicate_cursor    (cursor_t *cursor);
#endif