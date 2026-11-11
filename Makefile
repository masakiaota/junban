# Modified for junban in 2026 by junban contributors.
PREFIX ?= /usr/local
CPPFLAGS += -D_POSIX_C_SOURCE=200809L
CFLAGS ?= -std=c11 -Wall -Wextra -g -O2
OBJECTS = main.o visibility.o watch.o platform.o server.o server_start.o \
          client.o jobs.o execute.o msg.o error.o signals.o

junban: $(OBJECTS)
	$(CC) $(LDFLAGS) -o $@ $(OBJECTS) $(LDLIBS)

%.o: %.c main.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

.PHONY: test install uninstall clean
test: junban
	python3 tests/test_queue.py

install: junban
	install -d "$(PREFIX)/bin"
	install -m 755 junban "$(PREFIX)/bin/junban"

uninstall:
	rm -f "$(PREFIX)/bin/junban"

clean:
	rm -f *.o junban
