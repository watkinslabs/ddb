

#if !defined(_VALIDATE_H_)
    #define _VALIDATE_H_ 1
    #include "structure.h"

    int is_identifier_valid  (cursor_t * cursor,select_t *select,identifier_t *ident,char *section);
    int table_has_column     (table_def_t *table,char *column);
    int column_index_in_table(table_def_t* table, char* column);
    int validate_create_table(cursor_t * cursor,table_def_t *table);
    int validate_use         (cursor_t *cursor,use_t *use);
    int validate_select      (cursor_t * cursor,select_t *select);
    int add_identifier_to_cursor_lookup(cursor_t* cursor, select_t* select, identifier_t* ident);

#endif