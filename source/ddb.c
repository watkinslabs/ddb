#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "include/core.h"
#include "include/structure.h"
#include "include/teleprompter.h"
#include "include/warp_gate.h"
#include "include/lexer.h"
#include "include/debug.h"
#include <unistd.h>  



int main(int argc, char* argv[]) {

    char *query_str;
    // if not a terminal grab from pipe
    if (!isatty(0)) {   
      query_str=get_stdin();
    } else {  //args at this point
      if (argc==1){
        show_usage();
        return 0;
      }
      query_str=read_file(argv[1]);
    }

    // create a cursor and process queries
    cursor_t *cursor=init_cursor();
    process_queries(cursor,query_str);
    #if defined(DEBUG_ME)
    debug_cursor(cursor);
    #endif
    // free up leftover structures
    free(query_str);
    free_cursor(cursor);

    return 0;
}


/****
 * F5                for debug
 * CTRL + SHIFT + B  to build 
 * vscode requires launch.json andtask.json to be setup for this to work.
 * sofar the project is manually created as a command to gcc to build...
 ****/