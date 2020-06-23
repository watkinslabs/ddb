#include "structure.h"


#if !defined(_EXEC_H_)
    #define _EXEC_H_ 1

int execute_use          (cursor_t * cursor,use_t *use);
int execute_create_table (cursor_t * cursor,table_def_t *table);
int execute_select       (cursor_t * cursor,select_t *select);

#endif