TEST_SQL=test/simple.txt

.PHONY: func


build: 
	@clear
	@./build.sh


	@/usr/bin/gcc   -lm -g  builds/ddb.c -o builds/ddbc    -funroll-loops 
	#-ggdb -Wno-unused-variable -Wall 
	
	#-funroll-loops \
     #-pg  \
	 #-fsanitize=address -fno-omit-frame-pointer 
	#  -fsanitize=address -fno-omit-frame-pointer
	#

build-profile: 
	@/usr/bin/gcc -Wno-unused-variable -Wall -pg  builds/ddb.c  -o builds/ddbc

run:
	@echo builds/ddbc -d $(TEST_SQL)
	@builds/ddbc t< $(TEST_SQL)
	@gprof builds/ddbc gmon.out  > profile.txt

go: build run


profile: build-profile run
	@gprof builds/ddbc profiles/gmon.out > profiles/profile.txt

watch-time:
	@watch -n .3 'time make run  >/dev/null'

pipe:
	@cat test/sql/cli.txt | builds/ddbc 

cli:
	@builds/ddbc $(TEST_SQL)
