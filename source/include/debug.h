// debugging 

#include "tokens.h"

#if !defined (_DEBUG_H_)
    #define _DEBUG_H_ 1

    #define ANSI_COLOR_RED     "\x1b[31m"
    #define ANSI_COLOR_GREEN   "\x1b[32m"
    #define ANSI_COLOR_YELLOW  "\x1b[33m"
    #define ANSI_COLOR_BLUE    "\x1b[34m"
    #define ANSI_COLOR_MAGENTA "\x1b[35m"
    #define ANSI_COLOR_CYAN    "\x1b[36m"
    #define ANSI_COLOR_RESET   "\x1b[0m"


    void   gabble(char *source,char *msg);
    void   gobble(char *source,char *msg);
    void   goop(int depth,char *source,char *msg);
    char * vomit(int err_no);
    void   ghost(int err_no);
    char * string_duplicate(const char *str);
    char * sub_str_cpy(char *data,int start,int length);
    void   error(cursor_t *cursor,int ERR_NUM,char *message);

    void debug_alias(char *alias);
    void debug_header(char *title);
    void debug_sub_header(char *title);
    void debug_tuple(char *key,char* value);
    void debug_value(char* value);
    void debug_ordinal(int ordinal);


    void debug_expr        (expression_t *expr,int depth);
    void debug_identifier  (identifier_t *identifier);
    void debug_cursor      (cursor_t *cursor);
    void debug_select      (select_t *select);
    void debug_create_table(table_def_t *table);
    void debug_use         (use_t *use);
    void debug_dataset     (data_set_t *data);


 
#endif