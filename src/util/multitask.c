/*

@file      multitask.c

@brief     Utility functions for multiprocessing and multithreading

@author    Matt Lind
@date      Oct.2020
@copyright MIT License https://opensource.org/licenses/MIT

*/


#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>


#include "multitask.h"


/**
 
 @brief     Makes a parent process wait until all its child processes have exited
 
 @details   Beware that a parent process only waits for its DIRECT child processes but not for grand children.
 
 */

void wait_for_child_processes(void){
    while(wait(NULL) != -1 || errno != ECHILD);
}





/**
 
 @brief     Creates a shared memory that can be accessed by parent and children processes.
 
 @details   Beware: The memory must be manually 'unlinked' by the caller when it's not needed anymore.
 
 */

void *map_shared_memory(size_t mem_size){
    
    char *mem = mmap(0, mem_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    if (mem == MAP_FAILED){
        perror("[MMAP] Error creating shared memory! ABORT. ");
        exit(1);
    }

    memset(mem, 0, mem_size);
    
    return mem;
}




/**
 
 @brief     Loads a file into memory and maps it for shared, read-only access by parent and child processes.

 */

void *map_shared_file(const char *fname, size_t *fsize){
        
    struct stat fstat;

    if (stat(fname, &fstat) == -1) {perror("[STAT] Error getting file stats! ABORT. ");exit(1);}

    *fsize = fstat.st_size;
    
    int fd = open(fname, O_RDONLY);
    if (fd == -1) {perror("[OPEN] Error opening file for shared use! ABORT. ");exit(1);}
    
    char *mem = mmap((void*)0, *fsize, PROT_READ, MAP_SHARED, fd, 0);
    
    if (mem == (void*)(-1)) {perror("[MMAP] Error creating shared memory for file reading! ABORT. ");exit(1);}
    
    if (close(fd)==-1) {perror("[CLOSE] Error closing mapped file! ABORT. ");exit(1);}

    return mem;

}




/**
 
 @brief     Deallocates a shared memory.
  
 */

void unmap_shared_memory(void *mem, size_t size){
    
    
    int unmap_status = munmap(mem, size);
    
    if (unmap_status == -1) {
        perror("[MUNMAP] Error deallocating shared memory! ABORT. ");
        exit(1);
    }
    

}
