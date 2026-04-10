CC     = gcc
CFLAGS = -Wall -Wextra -O2 -pthread

all: node sender

node: node.c common.h
	$(CC) $(CFLAGS) -o $@ node.c

sender: sender.c common.h
	$(CC) $(CFLAGS) -o $@ sender.c

clean:
	rm -f node sender grid_ledger.csv /tmp/node_*.c /tmp/node_*.out \
	      /tmp/node_*_dir /tmp/node_*.tar.gz /tmp/grid_send.tar.gz

.PHONY: all clean
