#include <limits.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include "../include/errors.h"
#include "../include/tokens.h"
#include "../include/debug.h"


char *token_type(unsigned int t){
    switch(t){
        case  TOKEN_BLANK:           return "TOKEN_BLANK";
        case  TOKEN_ALPHA:           return "ALPHA";
        case  TOKEN_NUMERIC:         return "NUMERIC";
        case  TOKEN_STRING:          return "STRING";
        case  TOKEN_WHITESPACE:      return "WHITESPACE";
        case  TOKEN_SHIFT_LEFT:      return "SHIFT_LEFT";
        case  TOKEN_SHIFT_RIGHT:     return "SHIFT_RIGHT";
        case  TOKEN_SHORT_OR:        return "SHORT_OR";
        case  TOKEN_SHORT_AND:       return "SHORT_AND";
        case  TOKEN_NULL_EQ:         return "NULL_EQ";
        case  TOKEN_LESS_EQ:         return "LESS_EQ";
        case  TOKEN_GREATER_EQ:      return "GREATER_EQ";
        case  TOKEN_PLS_EQ:          return "PLS_EQ";
        case  TOKEN_MIN_EQ:          return "MIN_EQ";
        case  TOKEN_DIV_EQ:          return "DIV_EQ";
        case  TOKEN_MUL_EQ:          return "MUL_EQ";
        case  TOKEN_MOD_EQ:          return "MOD_EQ";
        case  TOKEN_LESS:            return "LESS";
        case  TOKEN_GREATER:         return "GREATER";
        case  TOKEN_PLUS:            return "PLUS";
        case  TOKEN_MINUS:           return "MINUS";
        case  TOKEN_DIVIDE:          return "DIVIDE";
        case  TOKEN_MULTIPLY:        return "MULTIPLY";
        case  TOKEN_NOT:             return "NOT";
        case  TOKEN_MODULUS:         return "MODULUS";
        case  TOKEN_ASSIGNMENT:      return "ASSIGNMENT";
        case  TOKEN_PAREN_LEFT:      return "PAREN_LEFT";
        case  TOKEN_PAREN_RIGHT:     return "PAREN_RIGHT";
        case  TOKEN_LIST_DELIMITER:  return "LIST_DELIMITER";
        case  TOKEN_DOT:             return "DOT";
        case  TOKEN_HEX:             return "HEX";
        case  TOKEN_BINARY:          return "BINARY";
        case  TOKEN_SELECT:          return "SELECT";
        case  TOKEN_DISTINCT:        return "DISTINCT";
        case  TOKEN_AS:              return "AS";
        case  TOKEN_WHERE:           return "WHERE";
        case  TOKEN_AND:             return "AND";
        case  TOKEN_OR:              return "OR";
        case  TOKEN_ORDER:           return "ORDER";
        case  TOKEN_GROUP:           return "GROUP";
        case  TOKEN_BY:              return "BY";
        case  TOKEN_LIMIT:           return "LIMIT";
        case  TOKEN_ASC:             return "ASC";
        case  TOKEN_DESC:            return "DESC";
        case  TOKEN_ON:              return "ON";
        case  TOKEN_IN:              return "IN";
        case  TOKEN_IS:              return "IS";
        case  TOKEN_LIKE:            return "LIKE";
        case  TOKEN_NULL:            return "NULL";
        case  TOKEN_FALSE:           return "FALSE";
        case  TOKEN_TRUE:            return "TRUE";
        case  TOKEN_UNKNOWN:         return "UNKNOWN";
        case  TOKEN_REAL:            return "REAL";
        case  TOKEN_NOT_EQ:          return "NOT_EQ";
        case  TOKEN_DELIMITER:       return "DELIMITER";
        case  TOKEN_BIT_OR:          return "BIT_OR";
        case  TOKEN_BIT_AND:         return "BIT_AND";
        case  TOKEN_LINE_COMMENT:    return "LINE_COMMENT";
        case  TOKEN_BLOCK_COMMENT:   return "BLOCK_COMMENT";
        case  TOKEN_NEW_LINE:        return "NEW_LINE";
        case  TOKEN_LINE_FEED:       return "LINE_FEED";
        case  TOKEN_TAB:             return "TAB";
        case  TOKEN_LITTERAL:        return "LITTERAL";
        case  TOKEN_IDENTIFIER:      return "IDENTIFIER";
        case  TOKEN_SUB_QUERY:       return "SUB_QUERY";
        case  TOKEN_FUNCTION:        return "FUNCTION";
        case  TOKEN_JOIN:            return "JOIN";
        case  TOKEN_LEFT_JOIN:       return "LEFT_JOIN";
        case  TOKEN_RIGHT_JOIN:      return "RIGHT_JOIN";
        case  TOKEN_FULL_OUTER_JOIN: return "FULL_OUTER_JOIN";
        case  TOKEN_INNER_JOIN:      return "INNER_JOIN";
        case  TOKEN_OUTER:           return "OUTER";
        case  TOKEN_INNER:           return "INNER";
        case  TOKEN_LEFT:            return "LEFT";
        case  TOKEN_RIGHT:           return "RIGHT";
        case  TOKEN_FULL:            return "FULL";
        case  TOKEN_GROUP_BY:        return "GROUP_BY";
        case  TOKEN_ORDER_BY:        return "ORDER_BY";
        case  TOKEN_QUALIFIER:       return "QUALIFIER";
        case  TOKEN_SOURCE:          return "SOURCE";
        case  TOKEN_LIMIT_START:     return "LIMIT_START";
        case  TOKEN_LIMIT_LENGTH:    return "LIMIT_LENGTH";
        case  TOKEN_BIT_EXPR:        return "BIT_EXPR";
        case  TOKEN_COLUMN_EXPR:     return "COLUMN_EXPR";
        case  TOKEN_SELECT_EXPR:     return "SELECT_EXPR";
        case  TOKEN_SIMPLE_EXPR:     return "SIMPLE_EXPR";
        case  TOKEN_EXPR_LIST:       return "EXPR_LIST";
        case  TOKEN_BOOLEAN_PRIMARY: return "BOOLEAN_PRIMARY";
        case  TOKEN_EXPR:            return "EXPR";
        case  TOKEN_ALIAS:           return "ALIAS";
        case  TOKEN_BOOLEAN:         return "BOOLEAN";
        case  TOKEN_COMPARITOR:      return "COMPARITOR";
        case  TOKEN_FROM:            return "FROM";
        case  TOKEN_IS_NULL:         return "IS_NULL";
        case  TOKEN_NOT_IN:          return "NOT_IN";
        case  TOKEN_IS_NOT_NULL:     return "IS_NOT_NULL";
        case  TOKEN_CREATE:          return "CREATE";
        case  TOKEN_TABLE:           return "TABLE";
        case  TOKEN_CREATE_TABLE:    return "CREATE_TABLE";
        case  TOKEN_FILE:            return "FILE";
        case  TOKEN_FIFO:            return "FIFO";
        case  TOKEN_REPO:            return "REPO";
        case  TOKEN_URL:             return "URL";
        case  TOKEN_ACCOUNT:         return "ACCOUNT";
        case  TOKEN_PASSWORD:        return "PASSWORD";
        case  TOKEN_BASE:            return "BASE";
        case  TOKEN_PATH:            return "PATH";
        case  TOKEN_PUSH:            return "PUSH";
        case  TOKEN_COMMIT:          return "COMMIT";
        case  TOKEN_PULL:            return "PULL";
        case  TOKEN_READ:            return "READ";
        case  TOKEN_COLUMN:          return "COLUMN";
        case  TOKEN_ARRAY_DELIMITER:  return "ARRAY_DELIMITER";
        case  TOKEN_COLUMN_DELIMITER: return "COLUMN_DELIMITER";
        case  TOKEN_QUOTED:           return "QUOTED";
        case  TOKEN_STRICT:           return "STRICT";
        case  TOKEN_REPO_PATH:        return "REPO_PATH";
        case  TOKEN_REPO_BASE:        return "REPO_BASE";
        case  TOKEN_PUSH_ON_COMMIT:   return "PUSH_ON_COMMIT";
        case  TOKEN_PULL_ON_COMMIT:   return "PULL_ON_COMMIT";
    }
    char *token_id=calloc(1,20);
    sprintf(token_id,"UNKNOWN: %d",(unsigned int)t);
    return token_id;
    
}



// function to create a tokens of given length. It initializes size of 
// tokens as 0 
token_array_t * token_array(unsigned length) { 
    token_array_t* tokens = ( token_array_t*)malloc(sizeof( token_array_t)); 
    tokens->length = length; 
    tokens->top = -1; 
    tokens->position=0;
    
    tokens->array = (token_t*)safe_malloc(sizeof(token_t),tokens->length);
    
    return tokens; 
} 
void token_add_type(token_array_t * arr,unsigned int type,unsigned int index){
    if(arr->array[index].depth<TOKEN_MAX_DEPTH){
        arr->array[index].expr[arr->array[index].depth]=type;
        ++arr->array[index].depth;
    }
}

int compare_token(token_array_t *tokens,unsigned int optional,unsigned int token){
    if(tokens->position>=tokens->length) return 0;

    token_t *t=&tokens->array[tokens->position];

    if(t==0) {
        return 0;
    }
    if(t->type==token) {
        ++tokens->position;
        return 1;
    } 
    if(optional==1) {
        return 1;
    }
    return 0;
}

void token_add_type_range(token_array_t * arr,unsigned int type,unsigned int index){
    for(int i=index;i<arr->position;i++){
        if(arr->array[i].depth<TOKEN_MAX_DEPTH){
            arr->array[i].expr[arr->array[i].depth]=type;
            ++arr->array[i].depth;
        }
    }
}

void token_set_type(token_array_t * arr,unsigned int type,unsigned int index){
    arr->array[index].type=type;
}


void tokens_destroy(token_array_t *tokens){
    for(int i=0;i<tokens->length;i++){
       if(tokens->array[i].value!=0) free(tokens->array[i].value);
    }
    free(tokens->array);
    free(tokens);
}

//return bool for a given index if in use 
int valid_token_index(token_array_t *tokens,unsigned int index){
    if(index>=0 && index<=tokens->top){
        return 1;
    }
    return 0;
}


//  token_array_t is full when top is equal to the last index 
int token_is_full(token_array_t* tokens) { 
    return tokens->top == tokens->length - 1; 
} 
  
//  token_array_t is empty when top is equal to -1 
int token_is_empty(token_array_t* tokens) { 
    return tokens->top == -1; 
} 
  
// Function to add an item to tokens.  It increases top by 1 
void token_push(token_array_t* tokens,unsigned int type,char *value) { 
    if (tokens->top == tokens->length - 1)  {
        ghost(ERR_TOKENS_FULL);
        return; 
    }
    ++tokens->top;
    tokens->array[tokens->top].type=type;
    tokens->array[tokens->top].value=value;
    
    //printf("%d pushed to tokens\n", item); 
} 
  
// Function to remove an item from tokens.  It decreases top by 1 
token_t token_pop(token_array_t* tokens) { 
    if (tokens->top == -1) {
        ghost(ERR_TOKENS_OUT_OF_BOUNDS);
    }
    token_t item=tokens->array[tokens->top--]; 
    //printf("%d popped from tokens\n", item); 
    return item;
} 
  
// selete a token in the array
void token_delete(token_array_t* tokens,unsigned int index) { 
    //printf("INDEX: %d of %d\n",index,tokens->length);
    if (tokens->top == -1) {
        ghost(ERR_TOKENS_OUT_OF_BOUNDS);
    }
    int section_length=sizeof(token_t)*(tokens->length-index-1);

    void *pos1_ptr=&tokens->array[index];
    void *pos2_ptr=&tokens->array[index+1];
    void *pos3_ptr=&tokens->array[tokens->length-1];

    void *buffer=malloc(section_length);
    
    // TODO make tokent_free-o-matic
    free(tokens->array[index].value);

    if(index!=(tokens->length-1)) {
        memcpy(buffer,pos2_ptr,section_length);
        memcpy(pos1_ptr,buffer,section_length);
    }
    
    memset (pos3_ptr,0,sizeof(token_t));
    free(buffer);
    if(tokens->position>=index) tokens->position--;
    tokens->top--;
} 

// Function to return the top from tokens without removing it 
token_t token_peek(token_array_t* tokens){ 
    if (tokens->top == -1) {
        ghost(ERR_TOKENS_OUT_OF_BOUNDS);
    }
    return tokens->array[tokens->top]; 
} 

void token_print(token_array_t *tokens){
    char* color;
    if (tokens!=NULL){
        for(int i=0;i<tokens->length;i++){
            if(tokens->array[i].value!=NULL){
                token_t token=tokens->array[i];
                
                if (token.type==TOKEN_ALPHA){

                color=ANSI_COLOR_RED;
                } else {
                    color=ANSI_COLOR_GREEN;
                }
                
                printf("[%d] %s %15s%s:%s \n",i,color,token_type(tokens->array[i].type),ANSI_COLOR_RESET,tokens->array[i].value);
            }
        }
    } else {
        ghost(ERR_TOKENS_EMPTY);
    }
}

//struct  token_array_t* tokens = createtokens(100); 
char * get_token_value(token_array_t *tokens,unsigned int index){
    return tokens->array[index].value;
}