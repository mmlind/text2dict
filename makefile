LIBS=-lpthread -lm 

text2dict: src/text2dict.c src/util/file.c src/util/multitask.c
	gcc -o bin/text2dict src/text2dict.c src/util/file.c src/util/multitask.c $(LIBS)
