/*

@file      file.c

@brief     Helper functions for dealing with files

@author    Matt Lind
@date      June 2016
@copyright MIT License https://opensource.org/licenses/MIT

 */

#include <stdlib.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include "file.h"




/**
 
 @brief     Check whether a file exists
 
 */

bool file_exists(const char* file_name){
    struct stat buffer;
    return (stat(file_name,&buffer)==0);
}




/**
 
 @brief     Get the file size in bytes
 
 */

size_t get_file_size(const char* file_name){
    struct stat buffer;
    if (stat(file_name,&buffer)!=0) {
        printf("File [%s] was not found! ABORT!\n",file_name);
        exit(1);
    }
    return (size_t)buffer.st_size;
}




/**
 
 @brief     Open a file, do error checking, and return the file handle

 @details   Closing must be done by the caller.
 
 */

FILE *open_file(const char *file_name, char *mode){
    
    FILE *fd = fopen (file_name, mode);
    if (fd == NULL) {
        int e = errno;
        printf("Unable to open the file: %s. ",file_name);
        printf("Error: %d  [%s] ABORT!\n",e,strerror(e));
        exit(1);
    }
    
    return fd;
}




/**
 
 @brief     Open and read a file into a malloc-allocated memory

 */

char *read_file_into_memory(const char *filename)
{
    long size = 0;
    FILE *f = open_file(filename, "rb");

    fseek(f, 0, SEEK_END);
    size = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *result = (char *)malloc(size+1);
    if (size != fread(result, sizeof(char), size, f))
    {
        free(result);
        printf("Reading the file into memory failed! ABORT!\n");
        exit(1);
    }
    fclose(f);

    return result;
}
