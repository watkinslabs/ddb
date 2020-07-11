


#if !defined(_CORE_H_)
    #define _CORE_H_ 1

    #include <limits.h> 
    #include <memory.h>
    #include <string.h>
    #include <stdio.h>
    #include <stdlib.h> 
    #include <stdarg.h>
    #include <math.h>
    #include <assert.h>
    #include <ctype.h>
    #include <time.h>

    
    #ifdef __linux__ 
        #define STRICMP stricmp
        #define STRCMP strcmp
        #define STRDUP strdup
        #define SPRINTF sprintf
        #define FOPEN fopen
        #define STRCAT strcat
        #define ISATTY isatty
        #define ACCESS access
    #elif _WIN32
        // windows code goes here
        #define STRICMP _stricmp
        #define STRCMP _strcmp
        #define STRDUP _strdup
        #define STRCAT strcat_s
        #define SPRINTF sprintf
        #define ISATTY _isatty
        #define ACCESS _access
        #define FOPEN fopen
        #define R_OK    4       /* Test for read permission.  */
        #define W_OK    2       /* Test for write permission.  */
        #define X_OK    R_OK    /* execute permission - unsupported in Windows,
                                use R_OK instead. */
        #define F_OK    0       /* Test for existence.  */

        #define CLOCK_GET_TIME win_clock_gettime

        typedef unsigned long       DWORD;

        typedef struct _FILETIME {
            DWORD dwLowDateTime;
            DWORD dwHighDateTime;
        } FILETIME, * PFILETIME, * LPFILETIME;
        #define CLOCK_REALTIME 0
        //struct timespec { long tv_sec; long tv_nsec; };    //header part
        
        static int win_clock_gettime(struct timespec* spec)      //C-file part
        {
            __int64 wintime=0; 
            GetSystemTimeAsFileTime((FILETIME*)&wintime);
            wintime -= 116444736000000000i64;  //1jan1601 to 1jan1970
            spec->tv_sec = wintime / 10000000i64;           //seconds
            spec->tv_nsec = wintime % 10000000i64 * 100;      //nano-seconds
            return 0;
        }


    #else
        #define STRICMP stricmp
        #define STRCMP strcmp
        #define STRDUP strdup
        #define SPRINTF sprintf
        #define FOPEN fopen
        #define STRCAT strcat
        #define ISATTY isatty
        #define ACCESS access

    #endif



    void * safe_malloc(int size,int qty);

#endif