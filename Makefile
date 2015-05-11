CC     = gcc
CFLAGS = -Wall -ansi -pedantic

AR = ar
AR_FLAGS = rcs
AR_IN = rvm.o
AR_OUT = librvm.a

RVM_IN = rvm.c
RVM_OUT = rvm.o
RVM_CFLAGS = -c

TEST_IN = $(TEST_FILE) librvm.a
TEST_OUT = test.o

all: librvm test

librvm: rvm.o
	$(AR) $(AR_FLAGS) $(AR_OUT) $(AR_IN)

rvm.o:  $(RVM_IN)
	$(CC) $(RVM_CFLAGS) -o $(RVM_OUT) $(CFLAGS) $(RVM_IN)

test:   $(TEST_IN)
	$(CC) -o $(TEST_OUT) $(CFLAGS) $(TEST_IN)
	./$(TEST_OUT)

clean:
	rm -f $(AR_OUT) $(RVM_OUT) $(TEST_OUT)
