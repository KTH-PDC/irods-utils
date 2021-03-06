
# Makefile is the make file for irods utilities.

# Postgres environment.
PGROOT=/usr/pgsql-9.4
PGLIB=$(PGROOT)/lib
PGINC=$(PGROOT)/include
LIBS=-lpq

# C switches. Debug and warnings on.
CSWITCH=-g -Wall -I$(PGINC) -L$(PGLIB)

# Distribution tarball.
DIST=~/iutil-0.1.tar.gz

# Install directories.
DESTDIR=/usr/local
DESTBIN=$DESTDIR/bin
DESTMAN=$DESTDIR/man/man1

# Test directories.
TD1=/snic.se/home/fconagy/x
TD2=/snic.se/home/fconagy

# Test output.
LIST=iutil.list
LF=$(LIST).files
LD=$(LIST).dir

# Default target.
all: ifind

# Cleanup.
clean:
	rm -f core
	rm -f ifind
	rm -f $(LIST)
	rm -f $(LF) $(LD)

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
	./ifind $(TD1) >$(LF)
	./ifind -v -s 3 $(TD1) >$(LF)
	./ifind -v -s 3 -D $(TD1) >$(LD)
	echo "========" >>$(LIST)
	./ifind -d 99 $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -d 99 $(TD1) -R 3,59,1024
	echo "========" >>$(LIST)
	./ifind -d 5 $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -v -s 1 $(TD1) >>$(LIST)
	./ifind -v -s 2 $(TD1) >>$(LIST)
	echo "======== -s 3" >>$(LIST)
	./ifind -v -s 3 $(TD1) >>$(LIST)
	echo "======== -s 4" >>$(LIST)
	./ifind -v -s 4 $(TD1) >>$(LIST)
	echo "======== -s 5 and 6 should fail" >>$(LIST)
	-./ifind -v -s 5 $(TD1)
	-./ifind -v -s 6 $(TD1)
	./ifind -s 3 -q -c 'echo' $(TD1) >$(LF).1
	diff $(LF) $(LF).1
	rm $(LF).1
	./ifind -D -s 3 -q -c 'echo' $(TD1) >$(LD).1
	diff $(LD) $(LD).1
	rm $(LD).1
	echo "========" >>$(LIST)
	./ifind -d 99 -c "echo xxxx '%s' 'x%s'" $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -S $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -S -D $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -S -u en_US.UTF8 $(TD2)
	./ifind -S -u C $(TD2) >>$(LIST)
	echo "========" >>$(LIST)
	echo "Number of directories" >>$(LIST)
	./ifind -D -v $(TD1) | wc -l >>$(LIST)
	./ifind -D -q -c echo $(TD1) | wc -l >>$(LIST)
	echo "Number of files" >>$(LIST)
	./ifind -v $(TD1) | wc -l >>$(LIST)
	./ifind -q -c echo $(TD1) | wc -l >>$(LIST)
	echo "========" >>$(LIST)
	echo "Parallel" >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -S -n 4 -c echo $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -S -n 32 -c echo $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -S -n 4 -b 32 -c echo $(TD1) >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -d 99 -S -n 4 -b 32 -c ./nullcmd $(TD1) | \
 egrep -v -e 'Running command|Build' >>$(LIST)
	./ifind -v -E demoResc $(TD1)
	echo "========" >>$(LIST)
	./ifind -v -X 'boost' /snic.se/home/fconagy/x >>$(LIST)
	./ifind -v -X 'boost' -Y 'XXXXX' /snic.se/home/fconagy/x >>$(LIST)
	echo "========" >>$(LIST)
	./ifind -v -l 256 /snic.se/home/fconagy/x >>$(LIST)
	echo "Tests finished check $(LIST)"

# Distribution kit.
dist: distclean
	(cd ..;  tar -cf $(DIST) ./iutil)

# End of file Makefile.

