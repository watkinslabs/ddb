//lexical tokenising

#include "tokens.h"

#if !defined(_LEXER_H_)
    #define _LEXER_H_ 1
    
    token_array_t *lex(char * query);
    void token_combine(token_array_t *tokens,int *list);
    void consolidate_tokens(token_array_t *tokens);
    int  process_queries(cursor_t *cursor,char *queries);
    void print_token_range(token_array_t *tokens,char * prefix,int start,int end);
    
#endif
