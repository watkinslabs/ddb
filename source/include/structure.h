
#if !defined(_STRUCTURE_H_)
    #define _STRUCTURE_H_ 1
    #include <time.h>
    #include "tokens.h"

//#define DEBUG_ME 1

#define LINE_ENDING '\n'
#define DOUBLE_QUOTE '\"'
#define SINGLE_QUOTE '\''
#define DATA_NULL "(zoink)"    

#define EVAL_STRING  1
#define EVAL_INT     2
#define EVAL_LONG    3
#define EVAL_FLOAT   4
#define EVAL_BOOL    5
#define EVAL_NULL    6
#define EVAL_DOUBLE  7

#define DEFAULT_COLUMN_DEIMITER ','


typedef struct identifier_t {
    char * qualifier;
    char * source;
} identifier_t;

typedef struct  data_column_t{
    int    type;
    int    ordinal;
    void * object;
    char * alias;
    struct  data_column_t *next;
    struct  data_column_t *next_tail;
} data_column_t;



typedef struct order_column_t {
    identifier_t * identifier;
    int direction;
    int ordinal;
} order_column_t;

typedef struct group_column_t {
    identifier_t * identifier;
    int ordinal;
} group_column_t;

typedef struct expression_t{
    int mode;
    int list;                 // expr list
    int not;                  // expr 
    int not_in;               // predicate 
    int in;                   // predicate
    int direction;            // column_list
    int assignment_operator;  // expression
    int comparison_operator;  // expression
    int arithmetic_operator;  // bit_expr
    int logical_operator;     // bit_expr
    int uinary_operator;      // bit_expr
    
    //this can be a LITTERAL OR a IDENTITY
    identifier_t *identifier;
    token_t      *literal;
    struct expression_t *expression;
    struct expression_t *expression_tail;
} expression_t;



typedef struct expression_value_t {
    char *STRING_V;
    int   INT_V;
    long  LONG_V;
    float FLOAT_V;
    int   type;
} expression_value_t;

typedef struct join_t {
    int             type;
    identifier_t  * identifier;
    char          * alias;
    expression_t  * expression;
    int             expression_length;
    int             ordinal;
} join_t;


typedef struct table_def_t{
    identifier_t *identifier;
    data_column_t *columns;
    char *file;
    /*
    char *base;
    char *fifo;
    // repo related items
    char *repo;
    char *url;
    char *account;
    char *password;
    char *repo_path;
    char *repo_base;
    int push_on_commit;
    int pull_on_read;
    */
    char *column;
    int strict;
    struct table_def_t *next;
    struct table_def_t *tail;

}table_def_t;



typedef struct command_t{
 int type;
 void *command;
 struct command_t *next;
 struct command_t *next_tail;
 
}command_t;

typedef struct use_t{
    char *database;
} use_t;

#define COLUMN_DATA       1
#define COLUMN_COMMENT    2
#define COLUMN_WHITESPACE 3

typedef struct row_t{
    char  **columns;
    long    column_length;
    int     column_type;
    long    file_row;
} row_t;

typedef struct data_set_t{
    char   ** columns;
    long      column_length;
    long      row_length;
    row_t  ** rows;
    long      position;
    int       success;
} data_set_t;


typedef struct identifier_lookup_t {
    identifier_t *identifier;
    long source;
    long source_column;
    long select_column;
    int active;
} identifier_lookup_t;


typedef struct cursor_t{
    //struct * variables;
    struct table_def_t  * active_table;
    struct table_def_t  * tables;
    char                * active_database;
    char                * requested_query;
    char                * executed_query;
    int                   parse_position;
    int                   error;
    char                * error_message;
    int                   status;
    struct timespec       created;
    struct timespec       ended;
    char               ** source_alias;     //array of source alias's
    
    data_set_t         ** source;           //array of datasets that results is created from
    int                   source_count;     //number of datasets in source
    data_set_t          * results;
    identifier_lookup_t * identifier_lookup;
    int                   identifier_count;

}cursor_t;


typedef struct range_t{
    long start;
    long end;
} range_t;


/****

identity comparitor expr
identity not in list
identity in list

 *****/


typedef struct select_t{
        // sub elements
        data_column_t   * columns;
        identifier_t    * from;
        join_t          * join;
        expression_t    * where;
        expression_t    * order;
        expression_t    * group;
        
        // elements
        char          * alias;
        int             distinct;
        int             limit_start;
        int             limit_length;

        // internal
        int             column_length;     
        int             join_length;     
        int             has_limit_length;
        int             has_limit_start;
    } select_t;
    

    select_t       * select_new();
  /*
    data_column_t  * data_column_list_init(int length);
    void             add_data_column(select_t *obj);
    void             data_column_init(data_column_t *column);
   */
    join_t          *join_list_init(int length);
    void             join_init(join_t *join);
    void             add_join(select_t *obj);

    order_column_t * order_column_list_init(int length);
    void             order_column_init(order_column_t *column);
    void             add_order_column(select_t *obj);
    group_column_t * group_column_list_init(int length);
    void             group_column_init(group_column_t *column);
    void             add_group_column(select_t *obj);

    void             set_distinct(select_t *obj);
    void             debug(token_array_t *tokens);

#endif