/*

@file      file.c

@brief     Helper functions for dealing with files

@author    Matt Lind
@date      June 2016
@copyright MIT License https://opensource.org/licenses/MIT

 */

#ifndef file_h
#define file_h

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>

bool file_exists(const char* file_name);

FILE *open_file(const char *file_name, char *mode);

size_t file_size(const char* file_name);


#endif /* file_h */

