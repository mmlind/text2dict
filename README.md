#  text2dict


A `C` program to ***quickly*** read a large text file (corpus) and create a list of its unique words (dictionary) and their respective count.

It was built as an exercise to compare different techniques for parallel processing:

- *multiprocessing* and memory synchronization via `semaphores`
- *multithreading* and memory synchronization via `mutexes`
- *multiprocessing* using `pipes` for IPC
- *multiprocessing* ***without*** memory synchronization and IPC


## How fast is it?

`text2dict` can convert the October 2020 17.3 GB English Wikipedia text corpus with 2.5 billion tokens into a dictionary of 9.9 million unique tokens in ***less than 1 minute***. 

It has a *fun* function `process_corpus_reading_only()` which simply reads all tokens in a corpus without creating a dictionary. In this mode processing the 2.5 billion tokens of the English Wikipedia on a cloud server with 128 processes takes only ***6 seconds***.


## Does it support non-alphabetic languages?

Yes. All tokens are read as UTF8 sequences. If you want to process character-based languages such as Chinese or Japanese you need to set the global variable `USE_SINGLE_CHARS_AS_TOKENS` to `true`.


## Converting Wikipedia dumps into token dictionaries

These are some of the Wikipedia language corpora that I tested `text2dict` on:

|Wikipedia language|token type|file size|tokens in corpus|avg. token length|tokens in dict|fastest creation of dict|fastest read of corpus|top 10 tokens
|:---|:---|---:|---:|---:|---:|---:|---:|:---|
|English   |word     |17.3 GB|2.8 billion| 6.2|9.9 million|50 seconds|6 seconds| the, of, and, in, to, was, is, for, on, as
|German    |word     | 7.5 GB|1.1 billion| 7.0|7.8 million|29 seconds|2 seconds|der, und, die, in, von, im, des, den kategorie, mit
|French    |word     | 6.3 GB|982 million| 6.4|4.9 million|25 seconds|2 seconds|de, la, le, et, en, du, des, les, est, un
|Spanish   |word     | 4.8 GB|743 million| 6.4|4.1 million|21 seconds| 1 second|de, la, en, el, del, que, los, se, por, un
|Italian   |word     | 3.9 GB|584 million| 6.7|3.7 million|19 seconds| 1 second|di, il, la, in, del, un, che, della, per, nel
|Portuguese|word     | 2.2 GB|324 million| 6.7|2.6 million|15 seconds| 1 second|de, em, do, da, que, no, um, com, uma, para
|Russian   |word     | 7.8 GB|554 million|14.1|6.6 million|26 seconds|2 seconds|на, года, категория, по, году, из, не, был, от, за
|Japanese  |character| 3.1 GB|944 million| 3.3|1.6 million|26 seconds|1  second|の, ー, ン, に, は, ス, た, ル, る, と
|Chinese   |character| 1.3 GB|406 million| 3.3|1.4 million|48 seconds|<1 second|的, 中, 一, 年, 大, 在, 人, 是, 有, 行


## How does it work?

For a detailed introduction and full code review please refer to my blog post: [How to use multiple cores to process very large text files](https://mmlind.github.io/posts/reading_wikipedia_in_seconds_or_how_to_use_multiple_cores_to_process_large_text_files/).


## Install

```
$ make
```

Before you build and run the code you need to choose what method of parallel processing you want to use by editing the `text2dict()` function in `text2dict.c`.
If you don't change anything it will use the `text2dict_mutexes()`  by default.

Beware that when using `text2dict_semaphores()` or `text2dict_pipes()` program execution will be ***significantly slower***. I don't recommend using either of the 2 methods for very large files. Refer to above blog post for details.  

## Usage

```
$ text2dict -p2 -m10 -i enwiki.txt -o dict.txt

@param     -i      INPUT  = file name and path of the text corpus that is to be read [REQUIRED]
@param     -o      OUTPUT = file name and path of the dictionary that is to be created [REQUIRED]

@param     -p      PROC   = number of processes that are run in parallel [DEFAULT: your machine's number of cores]
@param     -m      MIN    = minimum occurrences of a word for it to be included in the dictionary [DEFAULT: 1]

```
