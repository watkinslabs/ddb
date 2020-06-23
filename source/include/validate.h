#include "structure.h"


#if !defined(_VALIDATE_H_)
    #define _VALIDATE_H_ 1
    int is_identifier_valid  (cursor_t * cursor,select_t *select,identifier_t *ident,char *section);
    int table_has_column     (table_def_t *table,char *column);
    int validate_create_table(cursor_t * cursor,table_def_t *table);
    int validate_use         (cursor_t *cursor,use_t *use);
    int validate_select      (cursor_t * cursor,select_t *select);
#endif