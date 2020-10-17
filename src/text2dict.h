/*

@file      text2dict.h

@brief     Reads a large text file (corpus) and creates a list of all its unique words and their frequencies (dictionary)

@author    Matt Lind
@date      Oct.2020
@copyright MIT License https://opensource.org/licenses/MIT

*/



#ifndef text2dict_h
#define text2dict_h

#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>


//#if (defined(__GNU_LIBRARY__) && !defined(_SEM_SEMUN_UNDEFINED)) || defined(__FreeBSD__)
// union semun is not yet defined and thus needs to defined manually
#ifndef _SYS_SEM_H_
union semun {
    int             val;            // value for SETVAL
    struct semid_ds *buf;           // buffer for IPC_STAT & IPC_SET
    unsigned short  *array;         // array for GETALL & SETALL
};
typedef union semun semun_t;
#endif



// defines a segment via its first and last offset

typedef struct range_t{
    size_t from;    // included
    size_t to;      // excluded
} range_t;


// defines a token via its offset and length in the corpus (NOT null-terminated)

// basic structure to represent a 'word'
typedef struct corpus_token_t{
    size_t off;                 // offset of 1st occurrence in corpus
    int len;                    // length of the token, not null-terminated
    size_t count;               // counter for how many times this token is found
} corpus_token_t;




typedef int pipe_arr_t[2];

typedef struct pipe_t{
    int in;     // incoming
    int out;    // outgoing
} pipe_t;


typedef struct corpus_thread_t{
    char            *corpus;        // reference to corpus
    size_t          from_off;       // reference to hashtable
    size_t          to_off;         // reference to mutexes array
    corpus_token_t  *htab;          // reference to corpus segment start
    pthread_mutex_t *mtxs;          // reference to corpus segment end


} corpus_thread_t;



// used in unit testing only

int compare_corpus_tokens(const corpus_token_t *tok1, const corpus_token_t *tok2, const char *corpus);

corpus_token_t get_next_token(char *corpus, size_t *idx, size_t end);

range_t *create_corpus_segments(size_t corpus_size, long nsegs, char *corpus);



#endif /* text2dict_h */
