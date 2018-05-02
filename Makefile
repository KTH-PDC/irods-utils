# Makefile is the make file for irods utilities.

# Postgres environment.
PGROOT=/usr/pgsql-9.4
PGLIB=$(PGROOT)/lib
PGINC=$(PGROOT)/include
LIBS=-lpq

# C switches.
CSWITCH=-g -I$(PGINC) -L$(PGLIB)

# Distribution tarball.
DIST=~/iutil-0.1.tar.gz

# Install directories.
DESTDIR=/usr/local
DESTBIN=$DESTDIR/bin
DESTMAN=$DESTDIR/man/man1

# Test output.
LIST=files.list
TD1=/snic.se/home/fconagy/x
TD2=/snic.se/home/fconagy

# Default target.
all: ifind

# Cleanup.
clean:
	rm -f core
	rm -f ifind
	rm -f $(LIST)

distclean: clean

# Install.
install: ifind
	cp ifind $DESTBIN/
	cp ifind.1 $DESTMAN/

# Utility ifind executable.
ifind: ifind.c
	cc $(CSWITCH) -o ifind ifind.c $(LIBS)

# Test.
test: ifind
	echo "Starting tests"
	-./ifind
	-./ifind a
	-./ifind /b/
	-./ifind /nonexistent
	-./ifind -C "dbname=NODB user=nouser" '/snic.se/test'
	-./ifind -h
	./ifind -d 99 -q $(TD1) >$(LIST)
	./ifind -v $(TD1) >>$(LIST)
	./ifind -v -D $(TD1) >>$(LIST)
	./ifind -v -s 1 $(TD1) >>$(LIST)
	./ifind -v -S -s 2 $(TD1) >>$(LIST)
	-./ifind -v -s 3 $(TD1)
	./ifind -c 'echo' $(TD1) >>$(LIST)
	./ifind -d 99 -c "echo '%s'" $(TD1) >>$(LIST)
	./ifind -S $(TD1) >>$(LIST)
	./ifind -u en_US.UTF8 $(TD2)
	./ifind -u C $(TD2) >>$(LIST)
	./ifind -S -u C $(TD2) >>$(LIST)
	echo "Number of directories"
	./ifind -D -c echo $(TD1) | wc -l >>$(LIST)
	./ifind -S -D -c echo $(TD1) >>$(LIST)
	echo "Number of files"
	./ifind -c echo $(TD1) | wc -l >>$(LIST)
	./ifind -S $(TD1) >>$(LIST)
	./ifind -S -n 4 -c echo $(TD1) >>$(LIST)
	./ifind -S -n 32 -c echo $(TD1) >>$(LIST)
	./ifind -S -n 4 -b 32 -c echo $(TD1) >>$(LIST)
	./ifind -d 99 -S -n 4 -b 32 -c ./nullcmd $(TD1) | \
 egrep -v -e 'Running command|Build' >>$(LIST)
	echo "Tests finished check $(LIST)"

# Distribution kit.
dist: distclean
	(cd ..;  tar -cf $(DIST) ./iutil)

# End of file Makefile.

