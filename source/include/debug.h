// debugging 

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


 
#endif