# Make file for the ifind unility.

# Postgres environment.
PGROOT=/usr/pgsql-9.4
PGLIB=$(PGROOT)/lib
PGINC=$(PGROOT)/include
LIBS=-lpq

# Distribution tarball.
DIST=~/ifind-0.1.tar.gz

# Default target.
all: ifind

# Cleanup.
clean:
	rm -f core
	rm -f ifind

distclean: clean

# Our executable.
ifind: ifind.c
	cc -I$(PGINC) -L$(PGLIB) -o ifind ifind.c $(LIBS)

# Test.
test: ifind
	-./ifind
	-./ifind a
	-./ifind /b/
	-./ifind /nonexistent
	-./ifind -C "dbname=NODB user=nouser" '/snic.se/test'
	-./ifind -h
	./ifind -d 99 -q '/snic.se/home/fconagy/x'
	./ifind -v '/snic.se/home/fconagy/x'
	./ifind -v -D '/snic.se/home/fconagy/x'
	./ifind -v -s 1 '/snic.se/home/fconagy/x'
	./ifind -v -s 2 '/snic.se/home/fconagy/x'
	-./ifind -v -s 3 '/snic.se/home/fconagy/x'
	./ifind -c 'echo' '/snic.se/home/fconagy/x'
	./ifind -d 99 -c "echo '%s'" '/snic.se/home/fconagy/x'

dist:
	make distclean
	(cd ..;  tar -cf $(DIST) ./ifind)

