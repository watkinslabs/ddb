#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"
#include "../include/queries.h"

#define LINE_ENDING '\n'
#define DOUBLE_QUOTE '\"'
#define SINGLE_QUOTE '\''

int load_file(cursor_t *cursor,identifier_t *table_ident);

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



    if(select->from){
        load_file(cursor,select->from);
        if(select->join) {
            for(int i=0;i<select->join_length;i++) {
                load_file(cursor,select->join[i].identifier);
            }
        }
    }



    return 1;
}

int load_file(cursor_t *cursor,identifier_t *table_ident){
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
            char *err_msg=safe_malloc(1024,1);
            sprintf(err_msg,"cannot open file '%s'",table->file);
            error(cursor,ERR_FILE_OPEN_ERROR,err_msg);
            return 0;
        }

        //if no data abort
        if(data==0) {
            char *err_msg=safe_malloc(1024,1);
            sprintf(err_msg,"returned data empty. '%s'",table->file);
            error(cursor,ERR_DATA_FETCH_ERROR,err_msg);
            return 0;
        }
        
        //allocate dataset main container
        data_set_t * data_set=safe_malloc(sizeof(data_set_t),1);

        // count rows of data
        long lines=0;
        long last_line=0;
        for(long i=0;i<fsize;i++){
            if(data[i]==LINE_ENDING) {
                ++lines;
                last_line=i;
            }
        }
        
        //update data set and allocate row structure
        data_set->row_length=lines;
        data_set->rows=safe_malloc(sizeof(row_t),lines);

        int line=0;
        // quoted
        // strings
        // delimiter
        // array
        long i=0;
        char delimiter=table->column;
        while(i<fsize){
            // find next line ending
            int end_pos=0;
            int start_pos=0;
            for(int pos=i;pos<fsize;pos++){
                if(data[pos]==LINE_ENDING) {
                    end_pos=pos;
                    break;
                }
            }
            if(end_pos==0) end_pos=fsize;
            
            
            //scan the row and count the columns
            start_pos=i;
            int in_block=0;
            
            for(int pos=i;pos<end_pos;pos++){
                 //detect quoted string blocks
                 if(data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE) {
                     if(in_block==1) {
                         in_block=0;
                     } else {
                         in_block=1;
                     }
                     continue;
                 }

                 if(data[pos]==delimiter) {
                     ++data_set->rows[line].column_length;
                 }
            }//end row splitter
            
            // adding start column (off by 1)
            // ensures empty lines have 0 columns
            if(end_pos-i>0) {
                ++data_set->rows[line].column_length;
            }

            
            //allocate the correct abbout of column space
            data_set->rows[line].columns=safe_malloc(sizeof(char*), data_set->rows[line].column_length);
            
            //scan the row and duplicate the data into the columns
            start_pos=i;
            in_block=0;
            int ordinal=0;
            for(int pos=i;pos<end_pos;pos++){
                 //detect quoted string blocks
                 if(data[pos]==SINGLE_QUOTE || data[pos]==DOUBLE_QUOTE) {
                     if(in_block==1) {
                         in_block=0;
                     } else {
                         in_block=1;
                     }
                     continue;
                 }

                 if(data[pos]==delimiter) {
                     int len=start_pos-pos-1;
                     if(len>=0) {
                        void *value=safe_malloc(len+1,1);
                        if(len>0) {
                            memcpy(value,data[start_pos],len);
                        }
                        data_set->rows[line].columns[ordinal]=value;
                     }
                     ++ordinal;
                     start_pos=i+1;
                 }
            }//end row splitter
            ++line;
            i=end_pos+1;
        }// end main loop

        debug_dataset(data_set);

    
        return 1;
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

