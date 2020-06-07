
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/errors.h"
#include "../include/debug.h"

#define BUF_SIZE 512
#define BUF_MIN 128

char* read_file(char *file){
  FILE *fptr;
  char *buffer=0;
  long length=0;

   if ((fptr = fopen(file,"r")) == NULL){
       exit(ERR_FILE_OPEN_ERROR);
   }
    
  if (fptr) {
    fseek (fptr, 0, SEEK_END);
    length = ftell (fptr);
    fseek (fptr, 0, SEEK_SET);
    buffer = malloc (length+1);
    memset(buffer,0,length+1);
    if (buffer)    {
      fread (buffer, 1, length, fptr);
    } else {
      exit(ERR_MEMORY_ALLOCATION_ERR);
    }
    fclose (fptr);
  }
  return buffer;
}




char *get_stdin(){
  char *input, *p;
  int len, remain, n, size;

  size = BUF_SIZE;
  input = malloc(size);
  len = 0;
  remain = size;
  while (!feof(stdin)) {
      if (remain <= BUF_MIN) {
          remain += size;
          size *= 2;
          p = realloc(input, size);
          if (p == NULL) {
            free(input);
            return NULL;
          }
          input = p;
      }
      fgets(input + len, remain, stdin);
      n = strlen(input + len);
      len += n;
      remain -= n;
  }
  return input;
}//end get_stdin
