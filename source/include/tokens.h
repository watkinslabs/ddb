// tokens 

#if !defined(_TOKENS_H_)
    #define _TOKENS_H_ 1
    #define TOKEN_MAX_DEPTH        15

     #define TOKEN_BLANK            0  
     #define TOKEN_REAL             1  
     #define TOKEN_ALPHA            2  
     #define TOKEN_NUMERIC          3  
     #define TOKEN_BOOL             63  
     #define TOKEN_NULL             60  
     #define TOKEN_UNKNOWN          64  
     #define TOKEN_FALSE            61  
     #define TOKEN_TRUE             62  
     #define TOKEN_STRING           13  
     #define TOKEN_WHITESPACE       14  
     #define TOKEN_SHIFT_LEFT       15  
     #define TOKEN_SHIFT_RIGHT      16  
     #define TOKEN_SHORT_OR         17  
     #define TOKEN_SHORT_AND        18  
     #define TOKEN_NULL_EQ          19  
     #define TOKEN_LESS_EQ          20  
     #define TOKEN_GREATER_EQ       21  
     #define TOKEN_NOT_EQ           67  
     #define TOKEN_PLS_EQ           22  
     #define TOKEN_MIN_EQ           23  
     #define TOKEN_DIV_EQ           24  
     #define TOKEN_MUL_EQ           25  
     #define TOKEN_MOD_EQ           26  
     #define TOKEN_LESS             27  
     #define TOKEN_GREATER          28  
     #define TOKEN_PLUS             29  
     #define TOKEN_MINUS            30  
     #define TOKEN_DIVIDE           31  
     #define TOKEN_MULTIPLY         32  
     #define TOKEN_NOT              33  
     #define TOKEN_MODULUS          34  
     #define TOKEN_ASSIGNMENT       35  
     #define TOKEN_PAREN_LEFT       36  
     #define TOKEN_PAREN_RIGHT      37  
     #define TOKEN_LIST_DELIMITER   38  
     #define TOKEN_DOT              39  
     #define TOKEN_HEX              40  
     #define TOKEN_BINARY           41  
     #define TOKEN_SELECT           42  
     #define TOKEN_DISTINCT         43  
     #define TOKEN_FROM             44  
     #define TOKEN_AS               45  
     #define TOKEN_WHERE            46  
     #define TOKEN_AND              47  
     #define TOKEN_OR               48  
     #define TOKEN_ORDER            49  
     #define TOKEN_GROUP            50  
     #define TOKEN_BY               51  
     #define TOKEN_LIMIT            52  
     #define TOKEN_ASC              53  
     #define TOKEN_DESC             55  
     #define TOKEN_ON               57  
     #define TOKEN_LIKE             58  
     #define TOKEN_IN               65  
     #define TOKEN_IS               66  
     #define TOKEN_DELIMITER        70  
     #define TOKEN_BIT_OR           71  
     #define TOKEN_BIT_AND          72  
     #define TOKEN_LINE_COMMENT     73  
     #define TOKEN_BLOCK_COMMENT    74  
     #define TOKEN_NEW_LINE         75  
     #define TOKEN_LINE_FEED        76  
     #define TOKEN_TAB              77  
     #define TOKEN_ALIAS            79 // calculated;  
     #define TOKEN_EXPRESSION       80 // calculated;  
     #define TOKEN_LITTERAL         81 // calculated;  
     #define TOKEN_IDENTIFIER       82 // calculated;  
     #define TOKEN_SUB_QUERY        83 // calculated;  
     #define TOKEN_FUNCTION         84 // calculated ;  
     #define TOKEN_JOIN             56  
     #define TOKEN_LEFT_JOIN        85 // calculated;  
     #define TOKEN_RIGHT_JOIN       86 // calculated;  
     #define TOKEN_FULL_OUTER_JOIN  87 // calculated;  
     #define TOKEN_INNER_JOIN       88 // calculated;  
     #define TOKEN_OUTER            89  
     #define TOKEN_INNER            90  
     #define TOKEN_LEFT             91  
     #define TOKEN_RIGHT            92  
     #define TOKEN_FULL             93  
     #define TOKEN_GROUP_BY         94 // calculated;  
     #define TOKEN_ORDER_BY         95 // calculated;  
     #define TOKEN_QUALIFIER        96  
     #define TOKEN_SOURCE           97  
     #define TOKEN_BOOLEAN          98  
     #define TOKEN_COMPARITOR       99  
     #define TOKEN_LIMIT_START      101  
     #define TOKEN_LIMIT_LENGTH     102  
     #define TOKEN_BIT_EXPR         103  
     #define TOKEN_COLUMN_EXPR      104  
     #define TOKEN_SELECT_EXPR      105  
     #define TOKEN_SIMPLE_EXPR      106  
     #define TOKEN_EXPR_LIST        107  
     #define TOKEN_BOOLEAN_PRIMARY  108  
     #define TOKEN_EXPR             109  
     #define TOKEN_IS_NULL          110  // calculated ;  
     #define TOKEN_NOT_IN           112  // calculated;  
     #define TOKEN_IS_NOT_NULL      113  // calculated;  
     #define TOKEN_CREATE           114  
     #define TOKEN_TABLE            115  
     #define TOKEN_CREATE_TABLE     116  
     #define TOKEN_FILE             117  
     #define TOKEN_FIFO             118  
     #define TOKEN_REPO             119  
     #define TOKEN_URL              120  
     #define TOKEN_ACCOUNT          121  
     #define TOKEN_PASSWORD         122  
     #define TOKEN_BASE             123  
     #define TOKEN_PATH             124  
     #define TOKEN_PUSH             125  
     #define TOKEN_COMMIT           126  
     #define TOKEN_PULL             127  
     #define TOKEN_READ             128  
     #define TOKEN_COLUMN           129  
     #define TOKEN_ARRAY_DELIMITER  130  
     #define TOKEN_COLUMN_DELIMITER 131  
     #define TOKEN_ARRAY            132  
     #define TOKEN_QUOTED           133
     #define TOKEN_STRICT           134  
     #define TOKEN_REPO_PATH        135  // calculated;  
     #define TOKEN_REPO_BASE        136  // calculated;  
     #define TOKEN_PUSH_ON_COMMIT   137  // calculated;  
     #define TOKEN_PULL_ON_COMMIT   138  // calculated;  
     #define TOKEN_USE              139

    


    #define COLUMN_TYPE_IDENTIFIER 1
    #define COLUMN_TYPE_FUNCTION 1


    typedef struct token_t{
        unsigned int type;
        char *value;
        int expr[TOKEN_MAX_DEPTH];
        int depth;
    } token_t;

    
    // A structure to represent a stack 
    typedef struct token_array_t{ 
        int top; 
        unsigned length; 
        token_t * array; 
        int position;
        /*void * target;
        void * object;
        int    object_type;
        int has_object;*/

    } token_array_t; 




    token_array_t *  token_array          (unsigned length);
    void             tokens_destroy       (token_array_t * arr);
    int              valid_token_index    (token_array_t *tokens,unsigned int index);
    int              token_is_full        (token_array_t * arr);
    int              token_is_empty       (token_array_t * arr);
    void             token_push           (token_array_t * arr,unsigned int type,char *value);
    token_t          token_pop            (token_array_t * arr);
    token_t          token_peek           (token_array_t * arr);
    void             token_delete         (token_array_t* tokens,unsigned int index)
    ;
    void             token_print          (token_array_t * arr);
    void             token_add_type       (token_array_t * arr,unsigned int type,unsigned int index);
    void             token_add_type_range (token_array_t * arr,unsigned int type,unsigned int index);
    void             token_set_type       (token_array_t * arr,unsigned int type,unsigned int index);
    int              compare_token        (token_array_t *tokens,unsigned int optional,unsigned int token);


    char           * token_type        (unsigned int t);
    char           * get_token_value   (token_array_t * arr,unsigned int index);
#endif