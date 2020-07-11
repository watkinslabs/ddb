#if !defined(_FILES_H_)
    #define _FILES_H_ 1
    #include "structure.h"


    range_t * get_line (char *data,long *position,long fsize);
    row_t   * build_row(char *data,range_t *range,char delimiter);
//    int       lock_file(char *file);


#endif