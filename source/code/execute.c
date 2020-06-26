#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"
#include "../include/queries.h"

#define LINE_ENDING '\n'
#define DOUBLE_QUOTE '\"'
#define SINGLE_QUOTE '\''

data_set_t * load_file(cursor_t *cursor,identifier_t *table_ident);
data_set_t * new_data_set(char **columns,int column_count,int row_count);
char ** get_column_list(data_column_t *columns,int length);

/* Function: validate_create_table
 * -----------------------
 * execute the creatiopn of a table in the cuyrent cursor
 *
 * fail if:  
 *   null
 * returns: 1 for success
 *          zero or null otherwise
 */
int execute_create_table(cursor_t * cursor,table_def_t *table){
    
    // set most recent addition to active. append to end of list. update next and tail
    table_def_t *new_table=duplicate_table(table);
    if(cursor->tables==0)  {
        cursor->tables=new_table;
        cursor->tables->tail=new_table;
        cursor->active_table=new_table;
    } else {
        cursor->tables->tail->next=new_table;
        cursor->tables->tail=new_table;
        cursor->active_table=new_table;
    }

    return 1;
}

/* Function: execute_use
 * -----------------------
 * sets the curent database in the active cursor
 *
 * fail if:  
 *   null
 * returns: 1 for success
 *          zero or null otherwise
 */
int execute_use(cursor_t *cursor,use_t *use){
    if(use){
        // cleanup prior name
        if(cursor->active_database) free_string(cursor->active_database);
        // set curent name
        cursor->active_database=string_duplicate(use->database);
        return 1;
    }
    return 0;
}

int execute_select(cursor_t * cursor,select_t *select){
    /*
    Order of execution...
    1 FROM (including JOIN)
    2 WHERE 
    3 GROUP BY 
    4 HAVING 
    5 SELECT
        5.1 SELECT list
        5.2 DISTINCT
    6 ORDER BY 
    7 TOP / OFFSET-FETCH
    */
    int data_set_count=0;
    data_set_t **data_sets=0;
    if(select->from) ++data_set_count;
    data_set_count+=select->join_length;

    // for now we will load everything in 1 set of sets
    // phase 2 will be to cunk the data as needed
    // to minimize the memory foot print Such as blocks of 1 MB
    if(data_set_count>0) {
        data_sets=safe_malloc(sizeof(data_set_t),data_set_count);

        if(select->from){
            data_sets[0]=load_file(cursor,select->from);
            if(select->join) {
                for(int i=0;i<select->join_length;i++) {
                    data_sets[i+1]=load_file(cursor,select->join[i].identifier);
                }
            }
        }
    }

    
    // make result data set

    //loop through from and joins
    // only add rows that pass WHERE, then JOIN ON
    int row_count=0;
    char ** columns=get_column_list(select->columns,select->column_length);
    //data_set_t *results=new_data_set(columns,select->column_length,row_count);



    // free data sets
    if(data_set_count>0) {
        //ok we have the data we need. clear out the loaded data sets
        for(int i=0;i<data_set_count;i++) free_data_set(data_sets[i]);
        free(data_sets);
    }
    return 1;
}

char ** get_column_list(data_column_t *columns,int length){
    data_column_t *temp_data_column=columns;
    char **column_list=(char**)safe_malloc(sizeof(char*),length);
    long index=0;
    while (temp_data_column){
        column_list[index]=strdup(temp_data_column->object);
        temp_data_column=temp_data_column->next;
        ++index;
    }    
    return column_list;
}

range_t *get_line(char *data,long *position,long fsize) {
    if(*position>=fsize) {
        //printf("OUT OF BOUNDS %ld of %ld\n",*position,fsize);
        return 0;
    }


    range_t *range=(range_t*)safe_malloc(sizeof(range_t),1);
    range->end=0;
    range->start=*position;
    for(long pos=*position;pos<fsize;pos++){
        if(data[pos]==LINE_ENDING) {
            range->end=pos;
            break;        
        }
    }
    if(range->end==0) {
        range->end=fsize;
    }
    *position=range->end+1;
    return range;
}

row_t *build_row(char *data,range_t *range,char delimiter){
    // loop through range and split into columns
    row_t *row=(row_t*)safe_malloc(sizeof(row_t),1);
    
    int in_block=0;
    int start_pos=0;
    for(long pos=range->start;pos<range->end;pos++){
        //detect quoted string blocks
        //detect quoted string blocks
        if(start_pos==pos && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=1;
            continue;
        }
        if(in_block==1 && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=0;
            continue;
        }
        if(data[pos]==delimiter) {
            ++row->column_length;
            start_pos=pos+1;
        }
    }//end row splitter

    // adding start column (off by 1)
    // ensures empty lines have 0 columns
    if(range->end-range->start>0) {
        ++row->column_length;
    }
    //printf("%d \n",row->column_length);
    row->columns=(char**)safe_malloc(sizeof(char*),row->column_length+1);
    //scan the row and duplicate the data into the columns
    in_block=0;
    int ordinal=0;
    start_pos=range->start;
    for(long pos=range->start;pos<range->end;pos++){
        //detect quoted string blocks
        if(start_pos==pos && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=1;
            continue;
        }
        if(in_block==1 && (data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE)) {
            in_block=0;
            continue;
        }

        if(data[pos]==',' || pos+1==range->end) {
            int len=pos-start_pos;
            if(len>=0) {
                char *value=(char*)safe_malloc(len+1,1);
                if(len>0) {
                    memcpy(value,&data[start_pos],len);
                }
                row->columns[ordinal]=value;
            }
            ++ordinal;
            start_pos=pos+1;
        }
    }//end row splitter

    return row;
}


data_set_t *load_file(cursor_t *cursor,identifier_t *table_ident){

    table_def_t *table=get_table_by_identifier(cursor,table_ident);
    
    // does the table ident exist
    if(table) {
        // does not work at all the same wai in pytho
        // maybe i was just totally wrong.. wth?
       // lock_file(table->file);
        
        // read the file into a memory block in 1 chunk
        FILE *f = fopen(table->file, "rb");
        long fsize=0;
        char *data=0;
        if(f) {
            fseek(f, 0, SEEK_END);
            fsize = ftell(f);
            fseek(f, 0, SEEK_SET);

            data = malloc(fsize + 1);
            fread(data, 1, fsize, f);
            fclose(f);
            data[fsize] = 0;
        } else {
            char *err_msg=(char*)safe_malloc(1024,1);
            sprintf(err_msg,"cannot open file '%s'",table->file);
            error(cursor,ERR_FILE_OPEN_ERROR,err_msg);
            return 0;
        }

        //if no data abort
        if(data==0) {
            char *err_msg=(char*)safe_malloc(1024,1);
            sprintf(err_msg,"returned data empty. '%s'",table->file);
            error(cursor,ERR_DATA_FETCH_ERROR,err_msg);
            return 0;
        }
        
        //allocate dataset main container
        data_set_t * data_set=(data_set_t*)safe_malloc(sizeof(data_set_t),1);

        // count rows of data
        long lines=0;
        //long last_line=0;
        for(long i=0;i<fsize;i++){
            if(data[i]==LINE_ENDING) {
                ++lines;
                //last_line=i;
            }
        }
        
        //update data set and allocate row structure
        data_set->row_length=lines;
        data_set->rows=(row_t**)safe_malloc(sizeof(row_t),lines);

        int line=0;
        // quoted
        // strings
        // delimiter
        // array
        long i=0;
        char delimiter=',';
        if(table->column) delimiter=table->column[0];
        long position=1;
        

        
 
        range_t *range=get_line(data,&position,fsize);

        long index=0;
        long max_columns=0;
        while(range){
            //printf("Range %ld-%ld\n",range->start,range->end);

            row_t *row=build_row(data,range,delimiter);
            row->file_row=index;
            if(row->column_length>max_columns) max_columns=row->column_length;
            data_set->rows[index]=row;
            // TAIL 
            free(range);
            range=get_line(data,&position,fsize);
            ++index;
        }
        // free the data that was read        
        free(data);

        // init the column pointer lookup table
        data_set->columns=(char**)safe_malloc(sizeof(char*),max_columns);


        // copy column names for defined columns
        data_column_t * temp_data_column=table->columns;
        long ordinal=0;
        while(temp_data_column){
            data_set->columns[ordinal]=strdup(temp_data_column->object);
            temp_data_column=temp_data_column->next;
            ++ordinal;
        }
        // add default column names for undefined columns with `col_`+ordinal
        while(ordinal<max_columns){
            char *col_name=safe_malloc(sizeof(char),20);
            sprintf(col_name,"col_%ld",ordinal);
            data_set->columns[ordinal]=col_name;
            ++ordinal;
        }
        // update the max number of columns 
        // rows will have whatever they find
        // this refers to mas possible per row
        data_set->column_length=max_columns;
        //debug_dataset(data_set);
        return data_set;
    }
    return 0;
}




#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

int lock_file(char *file){
    struct sockaddr_un sun;
      if(strlen(file)>strlen(sun.sun_path)+1) return 0;
  
    int s = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s < 0) {
        
        return 0;
    }
    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    strcpy(sun.sun_path + 1,file);
    if (bind(s, (struct sockaddr *) &sun, sizeof(sun))) {
        perror("bind");
        return 0;
    }
    return 1;
}


 data_set_t *new_data_set(char **columns,int column_count,int row_count){

    //allocate dataset main container
    data_set_t * data_set=(data_set_t*)safe_malloc(sizeof(data_set_t),1);

    //update data set and allocate row structure
    data_set->row_length=row_count;

    // init the row pointer lookup table
    data_set->rows=(row_t**)safe_malloc(sizeof(row_t),row_count);

    // update the max number of columns 
    data_set->column_length=column_count;

    // init the column pointer lookup table
    data_set->columns=(char**)safe_malloc(sizeof(char*),column_count);

    // copy column names for defined columns
    for(int i=0;i<column_count;i++){
        data_set->columns[i]=strdup(columns[i]);
    }

    return data_set;
}