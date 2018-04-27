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
	./ifind -d 99 -q '/snic.se/home/fconagy/x' >$(LIST)
	./ifind -v '/snic.se/home/fconagy/x' >>$(LIST)
	./ifind -v -D '/snic.se/home/fconagy/x' >>$(LIST)
	./ifind -v -s 1 '/snic.se/home/fconagy/x' >>$(LIST)
	./ifind -v -S -s 2 '/snic.se/home/fconagy/x' >>$(LIST)
	-./ifind -v -s 3 '/snic.se/home/fconagy/x'
	./ifind -c 'echo' '/snic.se/home/fconagy/x' >>$(LIST)
	./ifind -d 99 -c "echo '%s'" '/snic.se/home/fconagy/x' >>$(LIST)
	./ifind -S '/snic.se/home/fconagy/x' >>$(LIST)
	./ifind -u en_US.UTF8 /snic.se/home/fconagy
	./ifind -u C /snic.se/home/fconagy >>$(LIST)
	./ifind -S -u C /snic.se/home/fconagy >>$(LIST)
	./ifind -D -c echo /snic.se/home/fconagy/x | wc -l

xxx:
	./ifind -S -D -c echo /snic.se/home/fconagy/x
	./ifind -c echo /snic.se/home/fconagy/x | wc -l
	./ifind -S /snic.se/home/fconagy/x
	./ifind -S -n 4 -c echo /snic.se/home/fconagy/x
	./ifind -S -n 32 -c echo /snic.se/home/fconagy/x
	./ifind -S -n 4 -b 32 -c echo /snic.se/home/fconagy/x
	echo "Tests finished check $(LIST)"

# Distribution kit.
dist: distclean
	(cd ..;  tar -cf $(DIST) ./iutil)

# End of file Makefile.

