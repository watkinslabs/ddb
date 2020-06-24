#include "../include/errors.h"
#include "../include/structure.h"
#include "../include/debug.h"
#include "../include/queries.h"
#include "../include/free.h"



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






    return 1;
}

int load_file(cursor_t *cursor,identifier_t table_ident){
    table_def_t *table=get_table_by_identifier(table_ident);
    if(table) {
        // does not work at all the same wai in pytho
        // maybe i was just totally wrong.. wth?
       // lock_file(table->file);
    
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