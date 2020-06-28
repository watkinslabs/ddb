


#if !defined(_STATEMENTS_H_)
    #define _STATEMENTS_H_ 1
    #include "structure.h"
    select_t    * process_select      (cursor_t *cursor,token_array_t *tokens,int *start);
    table_def_t * process_create_table(cursor_t *cursor,token_array_t *tokens,int *start);
    use_t       * process_use         (cursor_t *cursor,token_array_t *tokens,int *start);
#endif