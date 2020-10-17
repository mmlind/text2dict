/*
 
 @file      text2dict.c

 @version   1.0
 
 @brief     Reads a large text file (corpus) and creates a list (dictionary) of its unique words and their counts

 @param     -i      INPUT  = file name and path of the text corpus that is to be read [REQUIRED]
 @param     -o      OUTPUT = file name and path of the dictionary (txt file) that is to be created [REQUIRED]

 @param     -p      PROC   = number of processes that are run in parallel [DEFAULT: your machine's number of cores]
 @param     -m      MIN    = minimum occurrences of a word for it to be included in the dictionary [DEFAULT: 1]
 
 @author    Matt Lind [dnilttam @ outlook.com]
 
 @date      Oct.2020
 
 @copyright MIT License https://opensource.org/licenses/MIT

 @todo

 */

// #define _GNU_SOURCE     // allows to use fcntl with F_SETPIPE_SZ on linux (but doesn't work on Mac)

// system libraries
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/sem.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>


// project libraries
#include "text2dict.h"
#include "util/file.h"
#include "util/multitask.h"
#include "test/unit_test.h"


#define MAX_FILE_NAME_LEN 256         // maximum length of the corpus and dictionary files (incl. path)
#define MAX_TOKEN_STRING_LENGTH 256   // maximum length of a single word/token in the corpus
#define MAX_FILE_DESCRIPTORS 256      // needed for pipes. at least 2 * nproc + any other open fds
#define MAX_PROCESS_COUNT 200         // arbitrary limit for the number of processes -- to protect against wrong input
#define HASH_SIZING_FACTOR 470        // heuristic for defining hash size based on file size
#define SEMAPHORES_PER_SET 10000      // arbitrary threshold how many semaphores in a set are used
#define IS_UTF8(c) (((c)&0xC0)!=0x80) // check whether char c is the start of a UTF8 character

// compare two integers and return the smaller one
#define MIN_OF_TWO(a,b) ({ __typeof__ (a) _a = (a); __typeof__ (b) _b = (b);  _a < _b ? _a : _b; })

// UTF8 offsets for different positions/sizes in/of a UTF8 character (5-6 not in use anymore)
static const u_int32_t UTF8_OFFSETS[6] = {0x00000000UL, 0x00003080UL, 0x000E2080UL, 0x03C82080UL, 0xFA082080UL, 0x82082080UL};


// GLOBALS (to avoid having to pass them into many functions)
size_t MAX_HASH_LENGTH = 0;                 // will be dynamically set based on corpus file size
bool USE_SINGLE_CHARS_AS_TOKENS = false;    // count single characters as tokens -- set to 'true' for asian languages
long PROCESS_COUNT_READ_CORPUS;             // for PIPES option only -- number of processes reading the corpus
long PROCESS_COUNT_WRITE_HASH;              // for PIPES option only -- number of processes writing tokens into the hash table
long PROCESS_COUNT;                         // total number of processes or threads -- used by all options
long MIN_TOKEN_COUNT = 1;                   // minimum number of occurrences in corpus for a token to be added into dict
char CORPUS_FILE_NAME[MAX_FILE_NAME_LEN+1] = "";
char DICT_FILE_NAME  [MAX_FILE_NAME_LEN+1] = "";




/**
 
 @brief     Checks whether a UTF8 character, expressed via its unicode, is a token delimiter.
 
 @details   A UTF8 character is NOT necessarily a 'char' (aka single byte),
            and therefore referenced via its unicode (NOT char)
 
 */

bool is_token_delimiter(u_int32_t utf_code){
    
    return (bool) (
                   utf_code == 32    || // " " space

                   utf_code ==  9    || // " " tab
                   utf_code == 10    || // "\n" linefeed
                   utf_code == 13    || // "" carriage return
                   utf_code == 33    || // '!'
                   utf_code == 34    || // '"'
                   utf_code == 35    || // '#'
                   utf_code == 39    || // '''
                   utf_code == 40    || // '('
                   utf_code == 41    || // ')'
                   utf_code == 42    || // '*'
                   utf_code == 43    || // '+'
                   utf_code == 44    || // ','
                   utf_code == 46    || // '.'
                   utf_code == 47    || // '/'
                   utf_code == 58    || // ':'
                   utf_code == 59    || // ';'
                   utf_code == 60    || // '<'
                   utf_code == 61    || // '='
                   utf_code == 62    || // '>'
                   utf_code == 63    || // '?'
                   utf_code == 91    || // '['
                   utf_code == 92    || // '\'
                   utf_code == 93    || // ']'
                   utf_code == 95    || // '_'
                   utf_code == 123   || // '{'
                   utf_code == 124   || // '|'
                   utf_code == 125   || // '}'

                   utf_code == 8212  || // '—''
                   utf_code == 8216  || // '‘'
                   utf_code == 8217  || // '’'
                   utf_code == 8220  || // '“'
                   utf_code == 8221  || // '”'

                   utf_code == 12289 ||  // u8"、"
                   utf_code == 12290 ||  // u8"。"
                   
                   utf_code == 65281 ||  // u8"！"
                   utf_code == 65288 ||  // u8"（"
                   utf_code == 65289 ||  // u8"）"
                   utf_code == 65292 ||  // u8"，"
                   
                   utf_code == 65306 ||  // u8"："
                   utf_code == 65307 ||  // u8"；"
                   utf_code == 65311 ||    // u8"？"

                   (utf_code  >=  917600 && utf_code  <=  917699) ||
                   (utf_code  >= 1113000 && utf_code  <= 1113100)

                   );
    
}




/**
 
 @brief     Creates a malloc-allocated array of semaphore sets all initialized to 1
 
 @details   The number of semaphores per set as well as the size of the array overall
            is defined via 2 global variables/macros: MAX_HASH_LENGTH  and SEMAPHORES_PER_SET
 
 */

int *create_semaphore_sets(){
    
    int semset_count = (int)(MAX_HASH_LENGTH / SEMAPHORES_PER_SET) + 1;
    
    int *semset_ids = calloc(semset_count, sizeof(int));
    
    for (int i=0; i<semset_count; i++){
        if ((semset_ids[i] = semget(IPC_PRIVATE, SEMAPHORES_PER_SET, 0666 | IPC_CREAT))==-1) {perror("semget");exit(1);}
    }

    // reset all counters and semaphores
    semun_t semun = {.val = 1}; // initial semaphore value => 1 = released

    for (int setid=0; setid<semset_count; setid++){
        for (int semid=0; semid<SEMAPHORES_PER_SET; semid++){
            if(semctl(semset_ids[setid], semid, SETVAL, semun) == -1) {printf("semctl init for set %d sem %d: error %d \n",setid, semid, errno);exit(1);}
        }
    }

    return semset_ids;
}




/**
 
 @brief     Locks a semaphore by decrementing its value by 1
 
 */

void lock_semaphore(int *semset_ids, size_t hidx){
    
    int setid = (int)(hidx / SEMAPHORES_PER_SET);
    int semid = hidx % SEMAPHORES_PER_SET;

    struct sembuf sb;
    sb.sem_num = semid;
    sb.sem_flg = 0;
    sb.sem_op  = -1;  // lock token
    if (semop(semset_ids[setid], &sb, 1) == -1) {perror("semop");exit(1);}
    
}




/**
 
 @brief     Unlocks a semaphore by incrementing its value by 1
 
 */

void unlock_semaphore(int *semset_ids, size_t hidx){
    
    int setid = (int)(hidx / SEMAPHORES_PER_SET);
    int semid = hidx % SEMAPHORES_PER_SET;

    struct sembuf sb;
    sb.sem_num = semid;
    sb.sem_flg = 0;
    sb.sem_op  = 1;  // unlock token
    if (semop(semset_ids[setid], &sb, 1) == -1) {perror("semop");exit(1);}
    
}




/**
 
 @brief     Removes an array of semaphores sets from the OS register
 
 */

void remove_semaphore_sets(int *semset_ids){
    
    int semset_count = (int)(MAX_HASH_LENGTH / SEMAPHORES_PER_SET) + 1;
    
    for (int i=0; i<semset_count; i++){
        if (semctl(semset_ids[i], 0, IPC_RMID) == -1) {perror("semctl remove");exit(1);}
    }

}




/**
 
 @brief     Increase the OS limit on the number of simultaneous file descriptors
 
 @details   Only needed if you use a large number of PIPES
 
 */

void increase_pipe_limit(uint64_t new_max){
    
    struct rlimit plim;

    if (getrlimit(RLIMIT_NOFILE, &plim) == 0) printf("CURRENT limit for PIPES is [%lld] out of max [%lld]\n",plim.rlim_cur,plim.rlim_max);
    else {fprintf(stderr, "%s\n", strerror(errno));exit(1);}
    
    if (new_max<1 || new_max>plim.rlim_max) {printf("Pipe limit exceeds possible range! ABORT\n");exit(1);}
    
    // if current limit is higher then do nothing
    if (plim.rlim_cur >= new_max) return;
    
    plim.rlim_cur = new_max;
    
    if(setrlimit(RLIMIT_NOFILE, &plim) == -1)         fprintf(stderr, "%s\n", strerror(errno));
    
    if( getrlimit(RLIMIT_NOFILE, &plim) == 0)
        printf("NEW limit for PIPES is now [%lld] out of max [%lld]\n",plim.rlim_cur,plim.rlim_max);
    else
        fprintf(stderr, "%s\n", strerror(errno));
}




/**
 
 @brief     Creates a given number of pipes, returning a malloc allocated pointer.
 
 @details   Aborts if not at least 80% of the maximum file descriptor limit is still available.
 
 */

pipe_t *create_pipes(long npipes){
    
    // check that 80% capacity of the file descriptor limit is still available
    if (npipes>MAX_FILE_DESCRIPTORS*0.8){printf("Max limit for file descriptors is not sufficient. ABORT\n");exit(1);}
    
    pipe_t *pipes = malloc(npipes * sizeof(pipe_t));
    
    for (long i=0; i<npipes; i++){

        if (pipe(*(pipe_arr_t*)&pipes[i]) == -1){
            
//            fcntl(pipes[i].in, F_SETPIPE_SZ, 65536);
//            fcntl(pipes[i].out, F_SETPIPE_SZ, 65536);

            printf("Error when creating pipes. ABORT\n");
            exit(1);
        }
    }

    return pipes;
}




/**
 
 @brief     Closes the 'incoming' channel of all pipes
 
 */

void close_pipes_in(pipe_t *pipes, long npipes){
    
    for (long i=0; i<npipes; i++)
        if (close(pipes[i].in) == -1){printf("Error when closing 'in' pipes. ABORT\n");exit(1);}
}




/**
 
 @brief     Closes the 'outgoing' channel of all pipes
 
 */

void close_pipes_out(pipe_t *pipes, long npipes){
    
    for (long i=0; i<npipes; i++)
        if (close(pipes[i].out) == -1){printf("Error when closing 'in' pipes. ABORT\n");exit(1);}
}




/**
 
 @brief     Creates an array of 'corpus_thread_t' elements, all initialized with pointers to corpus, has htable and corpus segments.
 
 @details   Each 'corpus_thread_t' element is created for a specific thread, working on a specific corpus segment.
            The pointer to the mutexes is not yet added and must be added before the thread is launched.
 
 */

corpus_thread_t *create_corpus_threads(char *corpus, range_t *corp_segs, corpus_token_t *htab){
    
    corpus_thread_t *cts = malloc(PROCESS_COUNT * sizeof(corpus_thread_t));
    
    for (int i=0; i<PROCESS_COUNT; i++){
        
        cts[i].corpus    = corpus;                  
        cts[i].htab      = htab;                    
        cts[i].mtxs      = NULL;                    
        cts[i].from_off  = corp_segs[i].from;       
        cts[i].to_off    = corp_segs[i].to;         
    }
    
    return cts;
}




/**
 
 @brief     Finds the next token in a UTF8 string that is referenced via its start- and end-offset in a corpus.
 
 @details   The end of a word/token is reached when a "token delimiter" is found.
            The function returns the token and moves "off_pos" forward to the position where the delimiter was found.
            The function is repeatedly called until "off_pos" has reached "off_end".
 
            For character-based Asian languages the global variable USE_SINGLE_CHARS_AS_TOKENS
            should be set to TRUE to treat single characters as separate tokens.
 
 */

corpus_token_t get_next_token(char *corpus, size_t *idx, size_t end){

    corpus_token_t tok;
    size_t start_idx = *idx;
    
    // move IDX forward in case it points INSIDE of a multibyte character
    while (!IS_UTF8(corpus[*idx]) && (*idx < end) && corpus[*idx]) (*idx)++;
    
    // move IDX gradually forward
    while (*idx<end && *idx - start_idx<MAX_TOKEN_STRING_LENGTH && corpus[*idx] ){
        
        uint32_t u8_char_code = 0;
        int      u8_char_size = 0;
        
        do {
            u8_char_code <<= 6;
            u8_char_code += (unsigned char)corpus[*idx];
            u8_char_size++;
            (*idx)++;
        } while (*idx<end && corpus[*idx] && !IS_UTF8(corpus[*idx]));

        u8_char_code -= UTF8_OFFSETS[u8_char_size-1];

        // 3 exit conditions: (1) multi-byte character, (2) end of text, (3) token delimiter
        bool is_del = is_token_delimiter(u8_char_code);
        if (is_del || *idx==end || (USE_SINGLE_CHARS_AS_TOKENS && u8_char_size>1)) {
            tok.len   = is_del ? (int)(*idx - start_idx - u8_char_size) : (int)(*idx - start_idx);
            tok.off   = start_idx;
            tok.count = 1;
            return tok;
        }
        
    }
    
    // if no token was found, return empty token reference
    tok.len   = 0;
    tok.off   = 0;
    tok.count = 0;
    
    return tok;
}




/**
 
 @brief     Counts all filled buckets in the token hash table
 
 */

size_t get_htab_token_count(corpus_token_t *htab){
    
    size_t count = 0;
    
    for (size_t i=0; i<MAX_HASH_LENGTH; i++)
        if (htab[i].len>0) count++;

    return count;
}




/**
 
 @brief     Gets the string representation of a token object
 
 @details   The STR parameter must be sufficiently big to fit the string.
            Maximum token string size is defined via: MAX_TOKEN_STRING_LENGTH
 
 */

void get_token_string(corpus_token_t tok, char *str, char *corpus){
    
    memcpy(str, corpus + tok.off, tok.len);
    memset(str + tok.len, '\0', 1);

    // convert to lower case
    char *p = str;
    while ((*p = tolower(*p))) ++p;

    return;
}




/**
 
 @brief     Returns a hash for a given string

 @details   The size of the hash table is set via the global variable MAX_HASH_LENGTH.
 
 */

size_t get_token_hash(corpus_token_t tok, char *corpus){
    
    // get a character representation of the token
    char tok_str[MAX_TOKEN_STRING_LENGTH+1];
    get_token_string(tok, tok_str, corpus);
    
    char *str = tok_str;
    size_t hash = 5381;
    int c;

    while ((c = *str++)) hash = ((hash << 5) + hash) ^ c; // hash * 33 XOR c

    return hash % MAX_HASH_LENGTH;
        
}




/**
 
 @brief     Compares 2 tokens, both referenced by their corpus offset.
 
 @details   Used in the hash table insert function to check whether a new token already exists in the hash table.
 
 */

int compare_corpus_tokens(const corpus_token_t *tok1, const corpus_token_t *tok2, const char *corpus){
    
    int len1 = (*tok1).len;
    int len2 = (*tok2).len;

    int len = MIN_OF_TWO(len1, len2);
    
    const char *str1 = corpus + (*tok1).off;
    const char *str2 = corpus + (*tok2).off;

    int r = strncasecmp(str1, str2, len);
    
    // both strings differ in the first 'len' chars
    if (r!=0) return r;
    
    // both strings are the same
    if (len1 == len2) return 0;
    
    // if both strings have a different length but share the first 'len' characters
    // the shorter one is the smaller one
    
    if (len1 < len2) return -1;
    
    return 1;
}




/**
 
 @brief     Inserts a token into the hash table.
 
 @details   If the bucket referenced by the hash code is filled already the next free position is used.
            _mt = multithreading function, executed in  parallel by each thread
 
 @todo      Be careful: if the hash table is too small, this function may loop forever
 
 */

void insert_token_mt(long pidx, corpus_token_t tok, corpus_token_t *htab, char *corpus, pthread_mutex_t *mtxs){
    
    size_t hidx = get_token_hash(tok, corpus);
    
    while(1) {

        pthread_mutex_lock(&mtxs[pidx * MAX_HASH_LENGTH + hidx]);

        // check if this bucket is empty
        if (htab[hidx].len >0) {
            
            // if the same token already exists in the hash table increment its counter
            if (compare_corpus_tokens(&tok, htab+hidx, corpus) == 0) {
                htab[hidx].count += tok.count;
                pthread_mutex_unlock(&mtxs[pidx * MAX_HASH_LENGTH + hidx]);
                return;
                
            }
            // otherwise move forward to the next bucket
            else {
                pthread_mutex_unlock(&mtxs[pidx * MAX_HASH_LENGTH + hidx]);
                hidx++;
                hidx %= MAX_HASH_LENGTH;
            }
            
        } else {
            // if this token's bucket was empty (i.e. first time) add it into the hash table
            htab[hidx] = tok;
            pthread_mutex_unlock(&mtxs[pidx * MAX_HASH_LENGTH + hidx]);
            return;
        }
        
    }
    
}




/**
 
 @brief     Inserts a token into the hash table.
 
 @details   If the bucket referenced by the hash code is filled already the next free position is used.
            The last parameter SEMSET_IDS is only needed when semaphores are used for memory synchronization.
            Otherwise, just pass NULL.

            _mp = multiprocessing function, executed in  parallel by each child process

 @todo      Be careful: if the hash table is too small, this function may loop forever
 
 */

void insert_token_mp(long pidx, corpus_token_t tok, corpus_token_t *htabs, char *corpus, int *semset_ids){
    
    size_t hidx = get_token_hash(tok, corpus);
    size_t hidx_pp;
    
    while(1) {
        
        hidx_pp = hidx;
        
        if (semset_ids) lock_semaphore(semset_ids, hidx); // semaphore option

        // in case of multiple hash tables, find the correct one via the process idx
        else hidx_pp = (pidx * MAX_HASH_LENGTH) + hidx;
        
        // check if this bucket is empty
        if (htabs[hidx_pp].len >0) {
            
            // if the same token already exists in the hash table increment its counter
            if (compare_corpus_tokens(&tok, htabs + hidx_pp, corpus) == 0) {
                htabs[hidx_pp].count += tok.count;
                if (semset_ids) unlock_semaphore(semset_ids, hidx);
                return;
                
            }
            // otherwise move forward to the next bucket
            else {
                if (semset_ids) unlock_semaphore(semset_ids, hidx);
                hidx++;
                hidx %= MAX_HASH_LENGTH;
            }
            
        } else {
            // if this token's bucket was empty (i.e. first time) add it into the hash table
            htabs[hidx_pp] = tok;
            
            if (semset_ids) unlock_semaphore(semset_ids, hidx);
            return;
        }
        
    }
    
}




/**
 
 @brief     Inserts a token object into the token hash table.

 @details   Used for single process processing.
            Simply calls multiprocessing and passes 0 as process index.
  
 */

void insert_token(corpus_token_t tok, corpus_token_t *htab, char *corpus){
    insert_token_mp(0, tok, htab, corpus, NULL);
}




/**
 
 @brief     Splits a numeric range into equally sized segments and returns the [from..to] for each segment.

 @details   Beware that FROM is INCLUDED in the range, but TO is EXCLUDED.
            Therefore the TO from segment 1 will equal the FROM from segment 2, etc.
 */

range_t *create_segments(size_t total_size, long nsegs){
    
    if (total_size < nsegs){perror("Error creating segments. ABORT");exit(1);}

    range_t *segs = malloc(nsegs * sizeof(range_t));
    
    size_t seg_size = total_size / nsegs;
    
    for (long seg_idx=0; seg_idx<nsegs; seg_idx++){
        
        //  divide into equally-sized segments
        segs[seg_idx].from = seg_idx * seg_size;
        segs[seg_idx].to   = segs[seg_idx].from + seg_size;
        
        // always set the end of the last range to the end of the memory range
        if (seg_idx == nsegs-1) segs[nsegs-1].to = total_size;
        
    }
    
    return segs;
}




/**
 
 @brief     Returns the segment index in which a given token will be stored.

 @details   Used to identify what pipe (= what process = what hash segment)
            a token should be sent into.
  
 */

long get_segment_index(size_t pos, size_t total_size, long nsegs){
    
    size_t seg_size = total_size / nsegs;
    
    long seg_idx = pos / seg_size;
    
    return seg_idx;
}




/**
 
 @brief     Splits the corpus into equally-sized segments and returns the [from..to] offsets for each segment.

 @details   The split points for all segments are set by
            (1) assuming an equal size for each process/segment as default
            (2) if the default split point happens to be 'inside' a token (or UTF8 character)
            then the split point is moved forward until the next token delimiter.
  
 */

range_t *create_corpus_segments(size_t corpus_size, long nsegs, char *corpus){
    
    // first use default, i.e. divide into equally-sized segments
    range_t *segs = create_segments(corpus_size, nsegs);
    
    corpus_token_t tok;
    size_t pos;
    
    // move the split point to the right to the start of the next token
    for (long seg_idx=0; seg_idx<nsegs; seg_idx++){
        
        pos = segs[seg_idx].from;
        tok = get_next_token(corpus, &pos, corpus_size);

        // don't shift the 1st segment since it always starts at 0
        if (seg_idx>0){
            segs[seg_idx  ].from = pos;
            segs[seg_idx-1].to   = segs[seg_idx].from;
        }

    }

    return segs;
}




/**
 
 @brief     Processes a certain segment of the corpus = reads tokens and inserts them into a hash table

 @details   _mt = multithreading function, executed in  parallel by each thread.

 */

void *process_corpus_mt(void *arg){
    
    corpus_thread_t ct = *(corpus_thread_t*)arg;

    corpus_token_t tok;
    size_t str_pos = ct.from_off;
    size_t str_end = ct.to_off;

    while(str_pos < str_end){
        tok = get_next_token(ct.corpus, &str_pos, str_end);
        if (tok.len>0) insert_token_mt(0, tok, ct.htab, ct.corpus, ct.mtxs);
    }
    pthread_exit(NULL);
}




/**
 
 @brief     Processes the corpus via multithreading

 */

void process_corpus_multithreading(corpus_thread_t *cts){
    
    pthread_mutex_t *mtxs = malloc(MAX_HASH_LENGTH * sizeof(pthread_mutex_t));
    
    for (size_t i=0; i<MAX_HASH_LENGTH; i++) pthread_mutex_init (&mtxs[i], NULL);
    
    pthread_t threads[PROCESS_COUNT];
    for (long t=0; t<PROCESS_COUNT; t++){

        // add the mutexes pointer into the thread objects
        cts[t].mtxs = mtxs;
        
        if (pthread_create(&threads[t], NULL, process_corpus_mt, (void *)&cts[t])) {perror("pthread_create");exit(-1);}

    }
    
    // wait for threads to exit
    for (int t = 0;t<PROCESS_COUNT; t++) pthread_join(threads[t],NULL);

    // remove the mutexes pointers
    for (int i=0; i<PROCESS_COUNT; i++) cts[i].mtxs = NULL;
    free(mtxs);
}




/**
 
 @brief     Processes a certain segment of the corpus = reads tokens and inserts them into a hash table

 @details   _mp = multiprocessing function, executed in  parallel by each child process

 */

void process_corpus_mp(long pidx, char *corpus, size_t pos, size_t end, corpus_token_t *htabs, int *semset_ids){
    
    corpus_token_t tok;

    while(pos < end){
        tok = get_next_token(corpus, &pos, end);
        if (tok.len>0) insert_token_mp(pidx, tok, htabs, corpus, semset_ids);
    }

}




/**
 
 @brief     Processes the corpus via multiprocessing
 
 @details   Each process scans a separate memory segment.
            Tokens are inserted in a hash table (shared memory).
 
 */

void process_corpus_multiprocessing(char *corpus, size_t corpus_fsize, corpus_token_t *htabs, int *semset_ids){
    
    if (PROCESS_COUNT<2) {process_corpus_mp(0, corpus, 0, corpus_fsize, htabs, NULL);return;}
    
    range_t *segs = create_corpus_segments(corpus_fsize, PROCESS_COUNT, corpus);
    
    int pid;

    for (long pidx=0; pidx<PROCESS_COUNT; pidx++){

        if ((pid = fork()) < 0) {perror("fork");exit(1);}

        if (pid==0){
            process_corpus_mp(pidx, corpus, segs[pidx].from, segs[pidx].to, htabs, semset_ids);
            _exit(0);
        }
    }

    wait_for_child_processes();

    free(segs);
    
    return;
}




/**
 
 @brief     Process the corpus by simply reading its tokens (without creating a dictionary)
 
 @details   Only used for performance reference.
 
 */

void process_corpus_reading_only(char *corpus, size_t corpus_fsize){
    
    range_t *segs = create_corpus_segments(corpus_fsize, PROCESS_COUNT, corpus);
    
    int pid;
    
    for (long pidx=0; pidx<PROCESS_COUNT; pidx++){

        if ((pid = fork()) < 0) {perror("fork");exit(1);}

        if (pid==0){

            size_t pos =segs[pidx].from;
            size_t end =segs[pidx].to;

            corpus_token_t tok;
            while(pos < end){
                tok = get_next_token(corpus, &pos, end);
            }
            _exit(0);
        }
    }

    wait_for_child_processes();
    free(segs);
    return;
}




/**
 
 @brief     Reads tokens from a certain segment of the corpus and sends them into a pipe

 @details   _mp = multiprocessing function, executed in  parallel by each child process

 */

void read_corpus_and_send_tokens_into_pipe_mp(long pidx, char *corpus, range_t *corp_segs, pipe_t *pipes){
    
    close_pipes_in(pipes, PROCESS_COUNT_WRITE_HASH);

    size_t str_pos = corp_segs[pidx].from;
    size_t str_end = corp_segs[pidx].to;

    corpus_token_t tok;
    while(str_pos < str_end){
        tok = get_next_token(corpus, &str_pos, str_end);
        
        if (tok.len>0) {

            // find the index of the pipe/hash-segment that should be used for this token
            size_t hash = get_token_hash(tok, corpus);
            long seg_idx = get_segment_index(hash, MAX_HASH_LENGTH, PROCESS_COUNT_WRITE_HASH);

            // send the token into the selected pipe
            if (write(pipes[seg_idx].out, &tok, sizeof(corpus_token_t)) == -1) printf("Error sending token into the pipe!\n");
        }
    }
    close_pipes_out(pipes, PROCESS_COUNT_WRITE_HASH);
}




/**
 
 @brief     Reads tokens from a pipe and writes them into the hashtable.

 @details   _mp = multiprocessing function, executed in  parallel by each child process

 */

void read_pipe_and_write_tokens_into_htab(long pidx, char *corpus, corpus_token_t *htab, pipe_t *pipes){

    close_pipes_out(pipes, PROCESS_COUNT_WRITE_HASH);

    // read the pipe of this process until all sending is closed
    corpus_token_t tok;
    while (read(pipes[pidx].in, &tok, sizeof(tok))) insert_token(tok, htab, corpus);

    close_pipes_in(pipes, PROCESS_COUNT_WRITE_HASH);
}




/**
 
 @brief     Processes the corpus via pipes IPC
 
 @details   Spins-off 2 types of child processes:
            (1) processes that read the corpus and write them into a pipe
            (2) processes that read the pipe and writes the tokens in the hash table
            The number of reading and writing processes can be different.
            The number of pipes must match the number of processes that write into the hash table.
 
 */

void process_corpus_pipes(char *corpus, size_t corpus_fsize, range_t *corp_segs, corpus_token_t *htab, range_t *htab_segs){
    
    if (PROCESS_COUNT<2) {process_corpus_mp(0, corpus, 0, corpus_fsize, htab, NULL);return;}

    pipe_t *pipes = create_pipes(PROCESS_COUNT_WRITE_HASH);
   
    int pid;

    // Part 1 -- Kick-off the reading-corpus-processes
    for (long pidx=0; pidx<PROCESS_COUNT_READ_CORPUS; pidx++){

        if ((pid = fork()) < 0) {perror("fork");exit(1);}

        if (pid==0){
            read_corpus_and_send_tokens_into_pipe_mp(pidx, corpus, corp_segs, pipes);
            _exit(0);
        }
    }


    // Part 2 -- Kick-off the write-hash-processes
    for (long pidx=0; pidx<PROCESS_COUNT_WRITE_HASH; pidx++){

        if ((pid = fork()) < 0) {perror("fork");exit(1);}

        if (pid==0){
            read_pipe_and_write_tokens_into_htab(pidx, corpus, htab, pipes);
            _exit(0);
        }
    }

    // close parent process pipes
    close_pipes_in (pipes, PROCESS_COUNT_WRITE_HASH);
    close_pipes_out(pipes, PROCESS_COUNT_WRITE_HASH);

    wait_for_child_processes();
    
    free(pipes);
}




/**
 
 @brief     Merges multiple hash tables (must be based on same hash/key) into one..

 @details   Use multiprocessing.

 */

corpus_token_t *merge_hashtables(char *corpus, corpus_token_t *htabs){

    // create a new empty hash table that will hold the merged results of all processes' hash tables
    corpus_token_t *htab = map_shared_memory(MAX_HASH_LENGTH * sizeof(corpus_token_t));
    
    range_t *hsegs = create_segments(MAX_HASH_LENGTH, PROCESS_COUNT);

    int pid;

    for (long pidx=0; pidx<PROCESS_COUNT; pidx++){

        if ((pid = fork()) < 0) {perror("fork");exit(1);}

        if (pid==0){

            for (size_t hidx=hsegs[pidx].from; hidx<hsegs[pidx].to; hidx++){

                for (long hnum=0; hnum<PROCESS_COUNT; hnum++){
                    corpus_token_t tok = htabs[hnum * MAX_HASH_LENGTH + hidx];
                    if (tok.len>0) insert_token(tok, htab, corpus);
                }
            }
            _exit(0);
        }

    }

    wait_for_child_processes();

    free(hsegs);
    
    return htab;
}




/**
 
@brief      Compares 2 tokens by their frequency. Used in QSORT.
 
 */

int compare_token_counters(const void *a, const void *b){
    
    size_t freq_a = (*(corpus_token_t*)a).count;
    size_t freq_b = (*(corpus_token_t*)b).count;

    if (freq_a < freq_b) return 1;
    if (freq_a > freq_b) return -1;
    return 0;

}




/**
 
 @brief     Sorts the token dictionary based on each token's count (descending order)

 */

void sort_dict(corpus_token_t *dict, size_t dict_tok_count){
    qsort(dict, dict_tok_count, sizeof(corpus_token_t), compare_token_counters);
}
    
    


/**
 
 @brief     Extracts all filled buckets from the token hash table and puts them into an array that can be sorted
 
 */

corpus_token_t *create_dictionary(corpus_token_t *htab, char *corpus, size_t *dict_tok_count, size_t *corpus_tok_count){

    *dict_tok_count = get_htab_token_count(htab);
    
    *corpus_tok_count = 0;

    corpus_token_t *dict = calloc(*dict_tok_count, sizeof(corpus_token_t));

    size_t didx = 0;
    for (size_t hidx=0; hidx<MAX_HASH_LENGTH; hidx++){
    
        if (htab[hidx].len>0 && htab[hidx].count >= MIN_TOKEN_COUNT) {
            *corpus_tok_count += htab[hidx].count;  // calc overall token count
            dict[didx++] = htab[hidx];              // add token into dictionary
        }
        
    }
    
    // resize dictionary memory to actual size (in case MIN_TOKEN_COUNT was >1)
    dict = (corpus_token_t*) realloc(dict, didx * sizeof(corpus_token_t));
    *dict_tok_count = didx;
    
    return dict;
}




/**

 @brief     Creates a new text file with the unique words/tokens (one per line) followed by its respective count in the corpus.
  
 */

void write_dictionary_file(corpus_token_t *dict, size_t tok_count, char *corpus){

    FILE *dict_fd = open_file(DICT_FILE_NAME, "wb");

    char str[MAX_TOKEN_STRING_LENGTH+1];
    
    
    for (size_t i=0; i<tok_count; i++){
    
        get_token_string(dict[i], str, corpus);
        
        fputs(str, dict_fd);
        fputs(",", dict_fd);

        fprintf(dict_fd,"%'ld", dict[i].count);

        fputs("\n", dict_fd);
        
    }

    fclose(dict_fd);
}





/**
 
 @brief     Run TEXT2DICT in single process mode

 */

corpus_token_t *text2dict_single_process(char *corpus, size_t corpus_fsize, size_t *corpus_tok_count, size_t *dict_tok_count){
    
    PROCESS_COUNT = 1;
    corpus_token_t *htab = calloc(MAX_HASH_LENGTH, sizeof(corpus_token_t));
    process_corpus_multiprocessing(corpus, corpus_fsize, htab, NULL);
    corpus_token_t *dict = create_dictionary(htab, corpus, dict_tok_count, corpus_tok_count);
    free(htab);
    sort_dict(dict, *dict_tok_count);

    return dict;
}




/**
 
 @brief     Run TEXT2DICT in mode: multiple hashes (no memory synchronization, no IPC)

 */

corpus_token_t *text2dict_multi_hashes(char *corpus, size_t corpus_fsize, size_t *corpus_tok_count, size_t *dict_tok_count){
    
    corpus_token_t *htabs = map_shared_memory(PROCESS_COUNT * MAX_HASH_LENGTH * sizeof(corpus_token_t));
    process_corpus_multiprocessing(corpus, corpus_fsize, htabs, NULL);
    corpus_token_t *htab  = merge_hashtables(corpus, htabs);
    unmap_shared_memory(htabs, PROCESS_COUNT * MAX_HASH_LENGTH * sizeof(corpus_token_t));
    corpus_token_t *dict = create_dictionary(htab, corpus, dict_tok_count, corpus_tok_count);
    unmap_shared_memory(htab, MAX_HASH_LENGTH * sizeof(corpus_token_t));
    sort_dict(dict, *dict_tok_count);

    return dict;
}




/**
 
 @brief     Run TEXT2DICT using multiprocessing and memory synchronization via semaphores

 */

corpus_token_t *text2dict_semaphores(char *corpus, size_t corpus_fsize, size_t *corpus_tok_count, size_t *dict_tok_count){
    
    int *semset_ids = create_semaphore_sets();
    corpus_token_t *htab = map_shared_memory(MAX_HASH_LENGTH * sizeof(corpus_token_t));
    process_corpus_multiprocessing(corpus, corpus_fsize, htab, semset_ids);
    remove_semaphore_sets(semset_ids);
    corpus_token_t *dict = create_dictionary(htab, corpus, dict_tok_count, corpus_tok_count);
    unmap_shared_memory(htab, MAX_HASH_LENGTH * sizeof(corpus_token_t));
    sort_dict(dict, *dict_tok_count);

    return dict;
}




/**
 
 @brief     Run TEXT2DICT using multithreading and memory synchronization via mutexes

 */

corpus_token_t *text2dict_mutexes(char *corpus, size_t corpus_fsize, size_t *corpus_tok_count, size_t *dict_tok_count){
     
    range_t *segs = create_corpus_segments(corpus_fsize, PROCESS_COUNT, corpus);

    corpus_token_t *htab = calloc(MAX_HASH_LENGTH, sizeof(corpus_token_t));

    corpus_thread_t *corp_threads = create_corpus_threads(corpus, segs, htab);
    
    process_corpus_multithreading(corp_threads);
    
    corpus_token_t *dict = create_dictionary(htab, corpus, dict_tok_count, corpus_tok_count);

    sort_dict(dict, *dict_tok_count);
    
    free(corp_threads);
    free(htab);
    free(segs);
    
    return dict;
}




/**
 
 @brief     Run TEXT2DICT using multiprocessing and pipes for IPC

 */

corpus_token_t *text2dict_pipes(char *corpus, size_t corpus_fsize, size_t *corpus_tok_count, size_t *dict_tok_count){
    
    PROCESS_COUNT_READ_CORPUS = PROCESS_COUNT/2;  // half of the processes read the corpus
    PROCESS_COUNT_WRITE_HASH  = PROCESS_COUNT/2;  // half of the processes write the hashtable

    range_t *corp_segs = create_corpus_segments(corpus_fsize, PROCESS_COUNT_READ_CORPUS, corpus);
    range_t *htab_segs = create_segments(MAX_HASH_LENGTH, PROCESS_COUNT_WRITE_HASH);

    corpus_token_t *htab = map_shared_memory(MAX_HASH_LENGTH * sizeof(corpus_token_t));

    process_corpus_pipes(corpus, corpus_fsize, corp_segs, htab, htab_segs);
    
    corpus_token_t *dict = create_dictionary(htab, corpus, dict_tok_count, corpus_tok_count);
    sort_dict(dict, *dict_tok_count);
    
    // cleanup
    unmap_shared_memory(htab, MAX_HASH_LENGTH * sizeof(corpus_token_t));
    free(htab_segs);
    free(corp_segs);

    return dict;
}




/**
 
 @brief     Creates a dictionary text file containing all unique tokens and their counts  from a text corpus, using parallel processing

 @details   The corpus is divided into equally-sized text segments, and each process processes one of the segments.
            Tokens are stored as references (to their original position in the corpus) in a hash table.
            Token hash indexes are sorted in descending order by counts and written to a text file.
 
            BEWARE: The developer has to choose 1 of the 5 functions for sequential/parallel processing:
            - text2dict_single_process
            - text2dict_multi_hashes
            - text2dict_semaphores
            - text2dict_mutexes  (DEFAULT)
            - text2dict_pipes
 
 */

void text2dict(void){

    char tok_str[MAX_TOKEN_STRING_LENGTH+1];
    
    size_t corpus_fsize;
    char *corpus = map_shared_file(CORPUS_FILE_NAME, &corpus_fsize);
    
    MAX_HASH_LENGTH = 30000 * pow(2,log10(corpus_fsize));
    // Some alternative PRIME numbers to try as hash lengths: 13727587,18303449,32000011,36606883,40000003,53150323,80000023;
    // MAX_HASH_LENGTH = (corpus_fsize < 10000000) ? corpus_fsize : (corpus_fsize / HASH_SIZING_FACTOR);
    
    printf("Corpus file:           [%s] \n", CORPUS_FILE_NAME);
    printf("File size:             [%'15ld] \n", corpus_fsize);
    printf("# of procs/threads:    [%ld] \n", PROCESS_COUNT);
    
    size_t dict_tok_count   = 0;
    size_t corpus_tok_count = 0;

    // chose what method to use to produce the dictionary
    corpus_token_t *dict = text2dict_mutexes(corpus, corpus_fsize, &corpus_tok_count, &dict_tok_count);
    
    printf("Writing dict file...         \n");
    fflush(stdout);
    write_dictionary_file(dict, dict_tok_count, corpus);

    printf("# of tokens in corpus: [%'15lu]\n",corpus_tok_count);
    printf("# of tokens in dict:   [%'15lu]\n",dict_tok_count);

    printf("Top 10:\n");
    for (int i=0; i<10; i++) {
        get_token_string(dict[i], tok_str, corpus);
        printf("#%2d: %'12zu = %s\n",i+1,dict[i].count, tok_str);
    }

    // clean up
    free(dict);
    unmap_shared_memory(corpus, corpus_fsize);

    return;
}




/**

@brief     Main function -- command line interface of TEXT2DICT

@details   For usage and parameters see comments in file header
 
*/

int main(int argc, char *argv[]) {

    char *tmp;
    int param;

    setlocale(LC_ALL, "en_US.UTF-8");   // set locale (for outputting formatted numbers/dates/strings)
    
    PROCESS_COUNT              = sysconf(_SC_NPROCESSORS_ONLN); // default = # of processors

    // processing command line arguments
    while ((param = getopt (argc, argv, "i:o:p:m:s")) != -1) switch (param) {
        case 'i':
            strncpy(CORPUS_FILE_NAME, optarg, MAX_FILE_NAME_LEN);
            break;
        case 'o':
            strncpy(DICT_FILE_NAME, optarg, MAX_FILE_NAME_LEN);
            break;
        case 'm':
            MIN_TOKEN_COUNT = strtoul(optarg, &tmp, 10)<1 ? 1 : strtoul(optarg, &tmp, 10);
            break;
        case 'p': {
            unsigned long tmp_inp = strtoul(optarg, &tmp, 10);
            if (tmp_inp>0 && tmp_inp<=MAX_PROCESS_COUNT) {PROCESS_COUNT = tmp_inp;}
            else {printf("Invalid number of processes! The maximum on this machine is %ld.\n",PROCESS_COUNT);exit(1);}
            break;
        }
    }

    // display correct usage format if command line arguments were incomplete
    if (!CORPUS_FILE_NAME[0] || !DICT_FILE_NAME[0]) {printf("Usage: text2dict [-s] [-m #] [-p #] -i corpus.txt -o dict.cc \n");exit(1);}
    
    time_t start_time = time(NULL);     // remember the time in order to calculate processing time at the end
    text2dict();
    printf("\nProcessing time:       [%.0f sec]\n\n",difftime(time(NULL), start_time));
    
    return 0;
}
