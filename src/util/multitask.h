/*

@file      multitask.h

@brief     Utility functions for multiprocessing and multithreading

@author    Matt Lind
@date      Oct.2020
@copyright MIT License https://opensource.org/licenses/MIT

*/


#ifndef multitask_h
#define multitask_h

#include <stdio.h>



void wait_for_child_processes(void);

void *map_shared_memory(size_t mem_size);

void *map_shared_file(const char *fname, size_t *fsize);

void unmap_shared_memory(void *mem, size_t size);


#endif /* multitask_h */
