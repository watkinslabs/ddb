#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/queries.h"
#include "../include/debug.h"
#include "../include/free.h"


token_array_t *lex(char * query){
    unsigned int query_length=strlen(query);
    if (query_length<1){
        ghost(ERR_EMPTY_QUERY_STRING);

    }

    int new_word=0;
    char c,d,e;
    int skip=0;
    unsigned int t=0;
    int buffer_len=0;
    token_array_t *tokens=token_array(1000);
    char *new_token=0;

    if(1==1)

    for(int i=0;i<query_length;i++){

        buffer_len=query_length-i;
        c=query[i];

        //The start of a new token....
        
        if(new_word==0){
            if (buffer_len>2) d=query[i+1]; else  d=0;
            if (buffer_len>3) e=query[i+2]; else  e=0;
            skip=0;
            if      (c>='a' && c<='z')           { t=TOKEN_ALPHA;            }
            else if (c>='A' && c<='Z')           { t=TOKEN_ALPHA;            }
            else if (c=='\'')                    { t=TOKEN_STRING;           }
            else if (c=='"')                     { t=TOKEN_STRING;           }
            else if (c<=32)                      { t=TOKEN_WHITESPACE;       }
            else if (c=='0' && d=='x')           { t=TOKEN_HEX;              }
            else if (c=='0' && d=='X')           { t=TOKEN_HEX;              }
            else if (c=='0' && d=='b')           { t=TOKEN_BINARY;           }
            else if (c=='0' && d=='B')           { t=TOKEN_BINARY;           }
            else if (c>='0' && c<='9')           { t=TOKEN_NUMERIC;          }
            else if (c=='<' && d=='=' && e=='>') { t=TOKEN_NULL_EQ;         skip=2; }
            else if (c=='-' && d=='-')           { t=TOKEN_LINE_COMMENT;    skip=1; }
            else if (c=='/' && d=='*')           { t=TOKEN_BLOCK_COMMENT;   skip=1; }
            else if (c=='<' && d=='<')           { t=TOKEN_SHIFT_LEFT;      skip=1; }
            else if (c=='>' && d=='>')           { t=TOKEN_SHIFT_RIGHT;     skip=1; }
            else if (c=='|' && d=='|')           { t=TOKEN_SHORT_OR;        skip=1; }
            else if (c=='&' && d=='&')           { t=TOKEN_SHORT_AND;       skip=1; }
            else if (c=='<' && d=='=')           { t=TOKEN_LESS_EQ;         skip=1; }
            else if (c=='>' && d=='=')           { t=TOKEN_GREATER_EQ;      skip=1; }
            else if (c=='+' && d=='=')           { t=TOKEN_PLS_EQ;          skip=1; }
            else if (c=='-' && d=='=')           { t=TOKEN_MIN_EQ;          skip=1; }
            else if (c=='/' && d=='=')           { t=TOKEN_DIV_EQ;          skip=1; }
            else if (c=='*' && d=='=')           { t=TOKEN_MUL_EQ;          skip=1; }
            else if (c=='%' && d=='=')           { t=TOKEN_MOD_EQ;          skip=1; }
            else if (c=='!' && d=='=')           { t=TOKEN_NOT_EQ;          skip=1; }
            else if (c=='<' && d=='>')           { t=TOKEN_NOT_EQ;          skip=1; }
            else if (c=='<')                     { t=TOKEN_LESS;            skip=1; }
            else if (c=='>')                     { t=TOKEN_GREATER;         skip=0; }
            else if (c=='+')                     { t=TOKEN_PLUS;            skip=0; }
            else if (c=='-')                     { t=TOKEN_MINUS;           skip=0; }
            else if (c=='/')                     { t=TOKEN_DIVIDE;          skip=0; }
            else if (c=='*')                     { t=TOKEN_MULTIPLY;        skip=0; }
            else if (c=='!')                     { t=TOKEN_NOT;             skip=0; }
            else if (c=='%')                     { t=TOKEN_MODULUS;         skip=0; }
            else if (c=='=')                     { t=TOKEN_ASSIGNMENT;      skip=0; }
            else if (c=='(')                     { t=TOKEN_PAREN_LEFT;      skip=0; }
            else if (c==')')                     { t=TOKEN_PAREN_RIGHT;     skip=0; }
            else if (c==',')                     { t=TOKEN_LIST_DELIMITER;  skip=0; }
            else if (c=='.')                     { t=TOKEN_DOT;             skip=0; }
            else if (c==';')                     { t=TOKEN_DELIMITER;       skip=0; }
            else if (c=='|')                     { t=TOKEN_BIT_OR;          skip=0; }
            else if (c=='&')                     { t=TOKEN_BIT_AND;         skip=0; }
            else if (c=='\n')                    { t=TOKEN_NEW_LINE;        skip=0; }
            else if (c=='\r')                    { t=TOKEN_LINE_FEED;       skip=0; }
            else if (c=='\t')                    { t=TOKEN_TAB;             skip=0; }
    
            else t=0;

            switch(t){
                case TOKEN_ALPHA:
                    for(int lazer=i+1;lazer<query_length;lazer++){
                        c=query[lazer];
                        if (lazer!=query_length-1 && (
                            (c>='a' && c<='z') ||   
                            (c>='A' && c<='Z') ||
                            (c=='_' ) ||
                            (c>='0' && c<='9')) ) continue;
                        else {
                            new_token=sub_str_cpy(query,i,lazer-i);
                            skip=lazer-i-1;
                            break;
                        }
                    }
                    //one off.. yea clean this up TODO: dont be a bone head
                    if(new_token) { 
                        //new_token=sub_str_cpy(query,i,query_length-i);
                        //skip=query_length-i-1;
                    
                         
                        if (0==strncasecmp(new_token,"DISTINCT"  ,8 ) ) { t=TOKEN_DISTINCT;         } else
                        if (0==strncasecmp(new_token,"UNKNOWN"   ,7 ) ) { t=TOKEN_UNKNOWN;          } else
                        if (0==strncasecmp(new_token,"SELECT"    ,6 ) ) { t=TOKEN_SELECT;           } else
                        if (0==strncasecmp(new_token,"WHERE"     ,5 ) ) { t=TOKEN_WHERE;            } else
                        if (0==strncasecmp(new_token,"ORDER"     ,5 ) ) { t=TOKEN_ORDER;            } else
                        if (0==strncasecmp(new_token,"GROUP"     ,5 ) ) { t=TOKEN_GROUP;            } else
                        if (0==strncasecmp(new_token,"LIMIT"     ,5 ) ) { t=TOKEN_LIMIT;            } else
                        if (0==strncasecmp(new_token,"OUTER"     ,5 ) ) { t=TOKEN_OUTER;            } else
                        if (0==strncasecmp(new_token,"INNER"     ,5 ) ) { t=TOKEN_INNER;            } else
                        if (0==strncasecmp(new_token,"RIGHT"     ,5 ) ) { t=TOKEN_RIGHT;            } else
                        if (0==strncasecmp(new_token,"FALSE"     ,5 ) ) { t=TOKEN_FALSE;            } else
                        if (0==strncasecmp(new_token,"LEFT"      ,4 ) ) { t=TOKEN_LEFT;             } else
                        if (0==strncasecmp(new_token,"FULL"      ,4 ) ) { t=TOKEN_FULL;             } else
                        if (0==strncasecmp(new_token,"DESC"      ,4 ) ) { t=TOKEN_DESC;             } else
                        if (0==strncasecmp(new_token,"JOIN"      ,4 ) ) { t=TOKEN_JOIN;             } else
                        if (0==strncasecmp(new_token,"LIKE"      ,4 ) ) { t=TOKEN_LIKE;             } else
                        if (0==strncasecmp(new_token,"FROM"      ,4 ) ) { t=TOKEN_FROM;             } else
                        if (0==strncasecmp(new_token,"NULL"      ,4 ) ) { t=TOKEN_NULL;             } else
                        if (0==strncasecmp(new_token,"TRUE"      ,4 ) ) { t=TOKEN_TRUE;             } else
                        if (0==strncasecmp(new_token,"AND"       ,3 ) ) { t=TOKEN_AND;              } else
                        if (0==strncasecmp(new_token,"USE"       ,3 ) ) { t=TOKEN_USE;              } else
                        if (0==strncasecmp(new_token,"ASC"       ,3 ) ) { t=TOKEN_ASC;              } else
                        if (0==strncasecmp(new_token,"NOT"       ,3 ) ) { t=TOKEN_NOT;              } else
                        if (0==strncasecmp(new_token,"BY"        ,2 ) ) { t=TOKEN_BY;               } else
                        if (0==strncasecmp(new_token,"AS"        ,2 ) ) { t=TOKEN_AS;               } else
                        if (0==strncasecmp(new_token,"OR"        ,2 ) ) { t=TOKEN_OR;               } else
                        if (0==strncasecmp(new_token,"ON"        ,2 ) ) { t=TOKEN_ON;               } else
                        if (0==strncasecmp(new_token,"IS"        ,2 ) ) { t=TOKEN_IS;               } else
                        if (0==strncasecmp(new_token,"IN"        ,2 ) ) { t=TOKEN_IN;               } else 
                        if (0==strncasecmp(new_token,"DELIMITER" ,9 ) ) { t=TOKEN_COLUMN_DELIMITER; } else
                        if (0==strncasecmp(new_token,"PASSWORD"  ,8 ) ) { t=TOKEN_PASSWORD;         } else
                        if (0==strncasecmp(new_token,"ACCOUNT"   ,7 ) ) { t=TOKEN_ACCOUNT;          } else
                        if (0==strncasecmp(new_token,"COMMIT"    ,6 ) ) { t=TOKEN_COMMIT;           } else
                        if (0==strncasecmp(new_token,"COLUMN"    ,6 ) ) { t=TOKEN_COLUMN;           } else
                        if (0==strncasecmp(new_token,"QUOTED"    ,6 ) ) { t=TOKEN_QUOTED;           } else
                        if (0==strncasecmp(new_token,"STRICT"    ,6 ) ) { t=TOKEN_STRICT;           } else
                        if (0==strncasecmp(new_token,"CREATE"    ,6 ) ) { t=TOKEN_CREATE;           } else
                        if (0==strncasecmp(new_token,"ARRAY"     ,5 ) ) { t=TOKEN_ARRAY_DELIMITER;  } else
                        if (0==strncasecmp(new_token,"TABLE"     ,5 ) ) { t=TOKEN_TABLE;            } else
                        if (0==strncasecmp(new_token,"FILE"      ,4 ) ) { t=TOKEN_FILE;             } else
                        if (0==strncasecmp(new_token,"FIFO"      ,4 ) ) { t=TOKEN_FIFO;             } else
                        if (0==strncasecmp(new_token,"REPO"      ,4 ) ) { t=TOKEN_REPO;             } else
                        if (0==strncasecmp(new_token,"FILE"      ,4 ) ) { t=TOKEN_FILE;             } else
                        if (0==strncasecmp(new_token,"BASE"      ,4 ) ) { t=TOKEN_BASE;             } else
                        if (0==strncasecmp(new_token,"PATH"      ,4 ) ) { t=TOKEN_PATH;             } else
                        if (0==strncasecmp(new_token,"PUSH"      ,4 ) ) { t=TOKEN_PUSH;             } else
                        if (0==strncasecmp(new_token,"PULL"      ,4 ) ) { t=TOKEN_PULL;             } else
                        if (0==strncasecmp(new_token,"READ"      ,4 ) ) { t=TOKEN_READ;             } else
                        if (0==strncasecmp(new_token,"URL"       ,3 ) ) { t=TOKEN_URL;              } 











                    }
                    break;

                case TOKEN_NUMERIC: 

                    for(int lazer=i+1;lazer<query_length;lazer++){
                        c=query[lazer];
                        if (c>='0' && c<='9') continue;
                        else {
                            new_token=sub_str_cpy(query,i,lazer-i);
                            skip=lazer-i-1;
                            break;
                        }
                    }

                    break;

                case TOKEN_HEX: 
                    for(int lazer=i+2;lazer<query_length;lazer++){
                        c=query[lazer];
                        if ((c>='0' && c<='9') || ( (c>='a' && c<='f') || (c>='A' && c<='F') ) ) continue;
                        else {
                            new_token=sub_str_cpy(query,i,lazer-i);
                            skip=lazer-i-1;
                            break;
                        }
                    }
                    if(skip==0) ghost(ERR_MALFORMED_HEX_TOKEN);
                    if ( (c>'F' && c<='Z') || 
                         (c>'F' && c<='z') ) ghost(ERR_MALFORMED_HEX_TOKEN);
                    break;

                case TOKEN_BINARY: 
                    for(int lazer=i+2;lazer<query_length;lazer++){
                        c=query[lazer];
                        if (c=='0' || c=='1') continue;
                        else {
                            new_token=sub_str_cpy(query,i,lazer-i);
                            skip=lazer-i-1;
                            break;
                        }
                    }
                    if(skip==0) ghost(ERR_MALFORMED_BINARY_TOKEN);
                    if ( (c>'1' && c<='9') || 
                         (c>='A' && c<='Z') || 
                         (c>='a' && c<='z') ) ghost(ERR_MALFORMED_BINARY_TOKEN);

                    break;


                case TOKEN_WHITESPACE: 
                    for(int lazer=i+1;lazer<query_length;lazer++){
                        c=query[lazer];
                        if (c<32) continue;
                        else {

                            new_token=sub_str_cpy(query,i,lazer-i);
                            skip=lazer-i-1;
                            break;
                        }
                    }
                    break;
                case TOKEN_LINE_COMMENT:
                    //is it long enough
                    skip=-1;
                    for(int lazer=i+2;lazer<query_length;lazer++){
                        c=query[lazer];
                        if (c=='\n') {
                            new_token=sub_str_cpy(query,i+2,lazer-i-2);
                            skip=lazer-i-1;
                            break;
                        }
                    }
                    if(skip==-1){
                        printf("Error at: %d of %d\n",i,query_length);
                        ghost(ERR_UNTERMINATED_LINE_COMENT);
                    }
                    break;
                case TOKEN_BLOCK_COMMENT:
                    //is it long enough
                    if(query_length-i-3>0){
                        for(int lazer=i+2;lazer<query_length-1;lazer++){
                            c=query[lazer];
                            d=query[lazer+1];
                            
                            if (c=='*' && d=='/') {;
                                new_token=sub_str_cpy(query,i+2,lazer-i-2);
                                skip=lazer-i+1;
                                break;
                            }
                        }
                    } else {
                        ghost(ERR_UNTERMINATED_BLOCK_COMENT);
                    }

                    break;



                case TOKEN_STRING:
                    skip=-1;
                    for(int lazer=i+1;lazer<query_length;lazer++){
                        if(query[lazer]==c) {
                            // it's an empty string.....
                            if (lazer-i==1) 
                                new_token="";
                            else
                                new_token=sub_str_cpy(query,i+1,lazer-i-1);
                            skip=lazer-i;
                            break;
                        }
                    }
                    if (skip==-1) ghost(ERR_UNTERMINATED_STRING);
                break;
                case 0:     
                    printf("%c",c);
                    exit(ERR_UNKNOWN_SQL);
                    break;

                    
                default:
                    new_token=sub_str_cpy(query,i,skip+1);
                    break;
            }//end switch

            //we are not addng whitespace stuffs

            if (t!= TOKEN_WHITESPACE && 
                t!=TOKEN_LINE_COMMENT && 
                t!=TOKEN_BLOCK_COMMENT && 
                t!=TOKEN_NEW_LINE &&
                t!=TOKEN_TAB) {
                token_push(tokens,t,new_token);
                new_token=0;
            } else {
            
                if(new_token) {
                    free(new_token);
                }
                new_token=0;
            }
            
                
            i+=skip;
        }//end new word
    }
    consolidate_tokens(tokens);
    return tokens;

}


void token_combine(token_array_t *tokens,int *list){
    if (tokens==0)  return;
    if (list==0) return;

    int list_index=0;    
    int combo_token=list[1];
    int length=list[0];
    list=&list[2];
    for(int i=0;i<tokens->top;i++){
        if(tokens->array[i].type==list[list_index]) {
            ++list_index;
            if(list_index==length){
                int start_index=i-length+1;

                tokens->array[start_index].type=combo_token;
                int token_len=0;
                for(int w=0;w<length;w++) {
                    token_len+=strlen(tokens->array[start_index+w].value);
                }
                char *new_token=calloc(1,token_len+1);
                

                for(int w=0;w<length;w++){
                    strcat(new_token,tokens->array[start_index+w].value);
                }
                //free up replaced token value
                free(tokens->array[start_index].value);
                
                for(int w=1;w<length;w++){
                    token_delete(tokens,start_index+1);
                }
                tokens->array[start_index].value=new_token;
                i+=length-1;
                list_index=0;

            }
        } else {
            list_index=0;
        }
    }    
}


void consolidate_tokens(token_array_t *tokens){
    token_t d;
    token_t e;
    int buffer_len;

    //             length-2 combo token     match pattern
    int token1 [] ={3,TOKEN_FULL_OUTER_JOIN ,TOKEN_FULL          ,TOKEN_OUTER    ,TOKEN_JOIN     };
    int token2 [] ={3,TOKEN_IS_NOT_NULL     ,TOKEN_IS            ,TOKEN_NOT      ,TOKEN_NULL     };
    int token3 [] ={3,TOKEN_REAL            ,TOKEN_NUMERIC       ,TOKEN_DOT      ,TOKEN_NUMERIC  };
    int token4 [] ={2,TOKEN_NOT_IN          ,TOKEN_NOT           ,TOKEN_IN                       };
    int token5 [] ={2,TOKEN_REAL            ,TOKEN_DOT           ,TOKEN_NUMERIC                  };
    int token6 [] ={2,TOKEN_LEFT_JOIN       ,TOKEN_LEFT          ,TOKEN_JOIN                     };
    int token7 [] ={2,TOKEN_RIGHT_JOIN      ,TOKEN_RIGHT         ,TOKEN_JOIN                     };
    int token8 [] ={2,TOKEN_INNER_JOIN      ,TOKEN_INNER         ,TOKEN_JOIN                     };
    int token9 [] ={2,TOKEN_GROUP_BY        ,TOKEN_GROUP         ,TOKEN_BY                       };
    int token10[] ={2,TOKEN_ORDER_BY        ,TOKEN_ORDER         ,TOKEN_BY                       };
    int token11[] ={2,TOKEN_IS_NULL         ,TOKEN_IS            ,TOKEN_NULL                     };
    int token12[] ={2,TOKEN_CREATE_TABLE    ,TOKEN_CREATE        ,TOKEN_TABLE                    };
    
   // starts with a dot... REAL        
    token_combine(tokens,token1);
    token_combine(tokens,token2);
    token_combine(tokens,token3);
    token_combine(tokens,token4);
    token_combine(tokens,token5);
    token_combine(tokens,token6);
    token_combine(tokens,token7);
    token_combine(tokens,token8);
    token_combine(tokens,token9);
    token_combine(tokens,token10);
    token_combine(tokens,token11);
    token_combine(tokens,token12);

    //alias update
    for(int i=0;i<tokens->top;i++) {
        if(tokens->array[i].type== TOKEN_AS) {
            token_delete(tokens,i);
            if(tokens->top-i>1 && tokens->array[i].type==TOKEN_ALPHA){
                tokens->array[i].type=TOKEN_ALIAS;
            }
        } 
    }


    //identity update
    int length=tokens->top;
    for(int i=0;i<tokens->top;i++) {
        if(tokens->array[i].type== TOKEN_ALPHA) {
            if(length>1 && tokens->array[i+1].type== TOKEN_DOT){
                if(length>2 && tokens->array[i+2].type== TOKEN_ALPHA){
                    tokens->array[i].type=TOKEN_QUALIFIER;
                    tokens->array[i+2].type=TOKEN_SOURCE;
                    token_delete(tokens,i+1);
                    ++i;
                    continue;
                }
            }
            tokens->array[i].type=TOKEN_SOURCE;
        } 
        length--;
    }

    // limit fixup
    for(int i=0;i<tokens->top;i++) {
        if(tokens->array[i].type== TOKEN_LIMIT) {
            token_delete(tokens,i);

            if(i+2<tokens->top && 
                tokens->array[i  ].type==TOKEN_NUMERIC &&
                tokens->array[i+1].type==TOKEN_LIST_DELIMITER && 
                tokens->array[i+2].type==TOKEN_NUMERIC
                ) {
                    tokens->array[i  ].type=TOKEN_LIMIT_START;
                    tokens->array[i+2].type=TOKEN_LIMIT_LENGTH;
                    token_delete(tokens,i+1);
                    ++i;
                    continue;
                }
            if( tokens->array[i].type==TOKEN_NUMERIC){
                tokens->array[i].type=TOKEN_LIMIT_LENGTH;
            }
        }
    }//end limit fixup

    //create_table fixup
    // delete right handed assignments in tuple x=y
    for(int i=0;i<tokens->top-1;i++) {
        if(tokens->array[i].type== TOKEN_FILE || 
           tokens->array[i].type== TOKEN_STRICT || 
           tokens->array[i].type== TOKEN_COLUMN ) {
            ++i;
            if(tokens->array[i].type==TOKEN_ASSIGNMENT) {
                token_delete(tokens,i);
            }
        }
    }//end loop

    // duplicate delimiters fixup
    for(int i=0;i<tokens->top-1;i++) {
        //eat up sequential delimiters;
        if(tokens->array[i].type==TOKEN_DELIMITER) {
            ++i;
            while(tokens->array[i].type==TOKEN_DELIMITER) { 
                token_delete(tokens,i); 
            }
        }
    }
            
    

}//end funciton

command_t * add_command(command_t *command,void* item,int type){
    if(item==0) {
        printf ("ERR\n");
        return 0;
    }
    command_t *next=safe_malloc(sizeof(command_t),1);
    next->command=item;
    next->type=type;
    //empty list crete it
    if(command==0) {
        return next;
    }

    if(command->next==0) {
        command->next=next;
        command->next_tail=next;
    } else {
        command->next_tail->next=next;
        command->next_tail=next;
    }
    return command;
}

int process_queries(cursor_t *cursor,char *queries){
    // this tokenizes...
    //clear any error in this cursor. 
    if (cursor->error_message) free (cursor->error_message);
    cursor->error=0;
    cursor->error_message=0;
    token_array_t *tokens=lex(queries);
    int loop=1;

    command_t *commands=0;

    int position=0;
    // process queries and build data structure
    while(loop){
        position=tokens->position;
        select_t *select=process_select(cursor,tokens,&tokens->position);
        if(select) {
            commands=add_command(commands,select,TOKEN_SELECT);
        } else {
            if(position!=tokens->position) {
                break;
            }
        }

        position=tokens->position;
        table_def_t *table_def=process_create_table(cursor,tokens,&tokens->position);
        if(table_def) {
            commands=add_command(commands,table_def,TOKEN_CREATE_TABLE);
        } else {
            if(position!=tokens->position) {
                break;
            }
        }

        position=tokens->position;
        use_t *use=process_use(cursor,tokens,&tokens->position);
        if(use) {
            commands=add_command(commands,use,TOKEN_USE);
        } else {
            if(position!=tokens->position) {
                break;
            }
        }


        if(!compare_token(tokens,0,TOKEN_DELIMITER)){
            loop=0;
        } 
    }
    //printf("\nToken Count:%d of %d\n",tokens->position,tokens->top);
    
    // if the text isnt totally parsed.. then something is wrong....
    if(tokens->position<tokens->top){
        token_t *token=&tokens->array[tokens->position];
        int message_len=strlen(token->value)+100;
        char *message=safe_malloc(message_len,1);
        sprintf(message,"error: unknown text at position :%d %s >>> %s  <<< \n",tokens->position,token_type(token->type),token->value);
        error(cursor,ERR_UNKNOWN_SQL,message);
        return 0;
    }


    // if you've gotten this far the syntax is correct... 
    // validate / fixup data -> execute
    //   each command is validated then executed
    //   in order until finished or an error occurs
    if(cursor->error==0) {
        command_t * tmp_ptr=commands;
        command_t * tmp_ptr2;
        //doing this while no errors exist
        while(tmp_ptr){
            int res=0;
            // validate portion of the show
            switch(tmp_ptr->type){
                case TOKEN_CREATE_TABLE: res=validate_create_table(cursor,(table_def_t * )tmp_ptr->command); break;
                case TOKEN_SELECT      : res=validate_select      (cursor,(select_t    * )tmp_ptr->command); break;
                case TOKEN_USE         : res=validate_use         (cursor,(use_t       * )tmp_ptr->command); break;
            }
            if(cursor->error || res==0) break;
            // execute portion of the show
            switch(tmp_ptr->type){
                case TOKEN_CREATE_TABLE: res=execute_create_table(cursor,(table_def_t * )tmp_ptr->command); break;
                case TOKEN_SELECT      : res=execute_select      (cursor,(select_t    * )tmp_ptr->command); break;
                case TOKEN_USE         : res=execute_use         (cursor,(use_t       * )tmp_ptr->command); break;
            }
            if(cursor->error || res==0) break;
            tmp_ptr=tmp_ptr->next;
        }
    }


    // this happens no matter what
    // free all data structures
    command_t * tmp_ptr=commands;
    command_t * tmp_ptr2;
    // free resources;
    while(tmp_ptr){
        switch(tmp_ptr->type){
            case TOKEN_SELECT:       debug_select((select_t*)tmp_ptr->command);
                                     free_select((select_t*)tmp_ptr->command); 
                                     break;
            case TOKEN_USE:          debug_use((use_t*)tmp_ptr->command);
                                     free_use((use_t*)tmp_ptr->command); 
                                     break;
            case TOKEN_CREATE_TABLE: debug_create_table((table_def_t*)tmp_ptr->command);
                                     free_table_def((table_def_t*)tmp_ptr->command); 
                                     break;
            case TOKEN_BLANK:        printf("\n");
                                     break;

            default: printf("UNKOWN COMMAND\n");
        }
        tmp_ptr2=tmp_ptr;
        tmp_ptr=tmp_ptr->next;
        free(tmp_ptr2);
    }
    cursor->parse_position=tokens->position;
    tokens_destroy(tokens);
    clock_gettime(CLOCK_REALTIME,&cursor->ended);
    
    if(cursor->error) {
       // free_table_def(cursor->tables);
       // cursor->tables=temp_cursor->tables;
       // temp_cursor->tables=0;
       // free_cursor(temp_cursor);
        cursor->status=EXIT_FAILURE;
       return 0;
    } else {
        cursor->status=EXIT_SUCCESS;
        return 1;
    }
}


void print_token_range(token_array_t *tokens,char * prefix,int start,int end){
    for(int i=start;i<end;i++) {
        gabble(prefix,tokens->array[i].value);
    }
}



