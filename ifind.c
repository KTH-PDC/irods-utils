
/* IFIND.C executes a script/program for every file in a directory tree. */

/* System include files. */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

/* Postgres includes. Requires Postgres development packages. */
#include "libpq-fe.h"

/* Exit codes. */
#define SUCCESS ((int) 0)
#define FAILURE ((int) -1)

/* Boolean. */
typedef signed int boolean;

/* Boolean values. */
#define true ((int) 1)
#define false ((int) 0)

/* End of string character. */
#define EOS '\0'

/* Slash. */
#define SLASH '/'

/* Exit codes. */
#define PGRESEXEC ((int) 1)		/* Postgres execute error */
#define CANTCONNECT ((int) 2)		/* Cannot connect to database */

/* Path name length, this is difficult to tell. Arbitarily big. */
#define PATHNAME_LENGTH ((size_t) 65536)

/* Command length. */
#define COMMAND_LENGTH ((size_t) 65536)

/* Globals. */

/* Debug level. */
static int debug = 0;

/* Quiet. */
static int quiet = false;

/* Verbose level. */
static int verbose = 0;

/* Progress indicator. */
static int progress = 0;

/* Error exit. */

static void
err (int rc, char *format, ...)
{
	va_list args;

	/* Report error on stderr and exit. */
	va_start (args, format);
	(void) vfprintf (stderr, format, args);
	(void) fprintf (stderr, "\n");
	(void) fflush (stderr);
	va_end (args);
	exit (rc);
}

/* Print message. */

static void
msg (char *format, ...)
{
	va_list args;

	/* Print message on stdout with new line. */
	va_start (args, format);
	(void) vfprintf (stdout, format, args);
	(void) fprintf (stdout, "\n");
	(void) fflush (stdout);
	va_end (args);
}

/* Print message when not quiet. */

static void
info (char *format, ...)
{
	va_list args;

	/* Print message on stdout with new line. */
	if (! quiet)
	{
		va_start (args, format);
		(void) vfprintf (stdout, format, args);
		(void) fprintf (stdout, "\n");
		(void) fflush (stdout);
		va_end (args);
	}
}

/* Progress message. */

static void
pmsg (char *format, ...)
{
	va_list args;

	va_start (args, format);
	(void) vfprintf (stdout, format, args);
	(void) fflush (stdout);
	va_end (args);
}

/* Allocate with check. */

static void *
allocate (size_t s)
{
	void *r;

	r = malloc (s);
	if (r == NULL)
	{
		err (FAILURE, "Cannot allocate %llu bytes", s);
	}
	return (r);
}

/* Macro to allocate memory. */
#define new(t) ((t *) allocate (sizeof (t)))

/* Postgres error exit. */

static void
perr (int rc, PGconn *conn, char *format, ...)
{
	va_list args;
	char *pmsg;
	PGresult *res;

	/* Report error on stderr. */
	pmsg = PQerrorMessage(conn);
	va_start (args, format);
	(void) fprintf (stderr, "Postgres error: %s", pmsg);
	(void) vfprintf (stderr, format, args);
	(void) fprintf (stderr, "\n");
	(void) fflush (stderr);
	va_end (args);

	/* Cleanup and exit. Rollback will also close all cursors. */
	res = PQexec (conn, "ROLLBACK");
	PQclear(res);
	PQfinish (conn);
	exit (rc);
}

/* Execute Postgres command. */

PGresult *
pcmd (PGconn *conn, char *cmd)
{

	/* Command result. */
	PGresult *res;
	ExecStatusType pstat;

	/* Excecute Postgres command. */
	res = PQexec(conn, cmd);

	/* Ignore success. */
	pstat = PQresultStatus(res);
	if (pstat != PGRES_COMMAND_OK && pstat != PGRES_TUPLES_OK)
	{

		/* Cleanup and exit. */
		perr (PGRESEXEC, conn, "Error %s executing '%s'",
			PQresStatus(pstat), cmd);
	}
	return (res);
}

/* Postgres handle definition. */
typedef struct pghandle {

	/* Postgres connection structure. */
	PGconn *conn;

	/* Postgres result structure. */
	PGresult *res;

	/* Select statement command. */
	char *select_cmd;

	/* Fetch command. */
	char *fetch_cmd;

	/* Close command. */
	char *close_cmd;

	/* Number of database rows returned. */
	int nrows;

	/* Number of fields returned. */
	int nfields;
} pghandle_t;

/* Issue select for directories. */

pghandle_t *
select_directories (PGconn *conn, int sorted, int fetchcount,
	char *directory)
{

	/* Postgres handle returned. */
	pghandle_t *r;

	/* Wildchars to append for database LIKE string. */
	/* With a trailing slash it would be like char *wild = "/%"; */
	char *wild = "%";

	/* Database LIKE string length. */
	size_t like_len;

	/* Database LIKE string. */
	char *like;

	/* Collections select statement sprintf string. */
	char *colls_select;

	/* Collections select statement command created. */
	char *colls_cmd;

	/* Fetch select statement string. */
	char *fetch_select;

	/* Fetch command. */
	char *fetch_cmd;

	/* Close command. */
	char *close_cmd;

	/* Postgres exec result. */
	PGresult *res;

	/* Create handle. */
	r = new (pghandle_t);

	/* Build search string. Will add a LIKE clause. */
	like_len = (size_t) (strlen (directory) + strlen(wild));
	like = (char *) allocate (like_len + 1);
	*like = EOS;
	(void) strcpy (like, directory);
	(void) strcat (like, wild);

	/* Build sprintf string to create select statement. */
	colls_select = "DECLARE c CURSOR FOR SELECT coll_id,coll_name FROM \
r_coll_main WHERE coll_name LIKE '%s'";

	/* Create sprintf target, should be long enough. */
	colls_cmd = (char *) allocate (strlen (colls_select) + like_len + 1024);

	/* Create SQL statement. */
	(void) sprintf (colls_cmd, colls_select, like);
	free (like);

	/* Add sort clause if specified. */
	if (sorted == 0)
	{
		;
	}
	else if (sorted == 1)
	{
		strcat (colls_cmd,  " ORDER BY coll_name ASC");
	}
	else if (sorted == 2)
	{
		strcat (colls_cmd,  " ORDER BY coll_name DESC");
	}
	else
	{
		err (FAILURE, "Wrong sort option");
	}

	/* Build fetch SQL statement similarly. */
	fetch_select = "FETCH %d FROM c";
	fetch_cmd = (char *) allocate (strlen (fetch_select) + 1024);
	(void) sprintf (fetch_cmd, fetch_select, fetchcount);

	/* Close command. */
	close_cmd = strdup ("CLOSE c");
	if (close_cmd == NULL)
	{
		err (FAILURE, "Function strdup failed");
	}

	/* Issue the SQL. */
	res = pcmd (conn, colls_cmd);
	PQclear (res);

	/* Update return object. */
	r->conn = conn;
	r->res = NULL;
	r->select_cmd = colls_cmd;
	r->fetch_cmd = fetch_cmd;
	r->close_cmd = close_cmd;
	r->nrows = 0;
	r->nfields = 0;
	return (r);
}

/* Issue select for files in a directory. */

pghandle_t *
select_files (PGconn *conn, int sorted, int fetchcount,
	char *directory)
{

	/* Postgres handle returned. */
	pghandle_t *r;

	/* Files select statement sprintf string. */
	char *files_select;

	/* Length. */
	size_t files_len;

	/* Files select statement command created. */
	char *files_cmd;

	/* Fetch select statement string. */
	char *fetch_select;

	/* Fetch command. */
	char *fetch_cmd;

	/* Close command. */
	char *close_cmd;

	/* Postgres exec result. */
	PGresult *res;

	/* Create handle. */
	r = new (pghandle_t);

	/* Build sprintf string to create select statement. */
	files_select = "DECLARE d CURSOR FOR SELECT data_size,data_name \
FROM r_data_main WHERE coll_id=%s";
	files_len = (size_t) (strlen (files_select) +
		strlen(directory));

	/* Create sprintf target, should be long enough. */
	files_cmd = (char *) allocate (strlen (files_select) + files_len +
		1024);

	/* Create SQL statement. */
	(void) sprintf (files_cmd, files_select, directory);

	/* Add sort clause if specified. */
	if (sorted == 0)
	{
		;
	}
	else if (sorted == 1)
	{
		strcat (files_cmd,  " ORDER BY data_name ASC");
	}
	else if (sorted == 2)
	{
		strcat (files_cmd,  " ORDER BY data_name DESC");
	}
	else
	{
		err (FAILURE, "Wrong sort option");
	}

	/* Build fetch SQL statement similarly. */
	fetch_select = "FETCH %d FROM d";
	fetch_cmd = (char *) allocate (strlen (fetch_select) + 1024);
	(void) sprintf (fetch_cmd, fetch_select, fetchcount);

	/* Close command. */
	close_cmd = strdup ("CLOSE d");
	if (close_cmd == NULL)
	{
		err (FAILURE, "Function strdup failed");
	}

	/* Issue the SQL. */
	res = pcmd (conn, files_cmd);
	PQclear (res);

	/* Update return object. */
	r->conn = conn;
	r->res = NULL;
	r->select_cmd = files_cmd;
	r->fetch_cmd = fetch_cmd;
	r->close_cmd = close_cmd;
	r->nrows = 0;
	r->nfields = 0;
	return (r);
}

/* Issue fetch. */

void
fetch (pghandle_t *h)
{

	/* Free memory from the previous fetch if any. */
	PQclear (h->res);

	/* Issue the fetch. */
	h->res = pcmd (h->conn, h->fetch_cmd);

	/* Update indicators. */
	h->nfields = PQnfields (h->res);
	h->nrows = PQntuples (h->res);
	if (debug > 5)
	{
		if (h->nrows != 0)
		{
			msg ("    %s %8d", h->fetch_cmd, h->nrows);
		}
	}
}

/* Issue close cursor and finish select. */

void
closecursor (pghandle_t *h)
{

	/* Issue the close cursor. */
	h->res = pcmd (h->conn, h->close_cmd);

	/* Free memory. */
	PQclear (h->res);

	/* Free more memory, leave conn and res. */
	free (h->select_cmd);
	free (h->fetch_cmd);
	free (h->close_cmd);
	h->select_cmd = NULL;
	h->fetch_cmd = NULL;
	h->close_cmd = NULL;
	h->nrows = 0;
	h->nfields = 0;
}

/* Show progress */

void
show_progress (unsigned long rno)
{
	if (progress > 0)
	{
		if ((rno % ((unsigned long) progress)) == (unsigned long) 0)
		{
			pmsg (".");
		}
	}
}

/* Execute command with pathname. */

void
do_command (char *command, char *pathname)
{
	char *s;
	char *state;
	char *token;
	int formats;
	char cmd[COMMAND_LENGTH];
	int status;

	/* Enter command function. */
	if (debug > 5)
	{
		msg ("do command '%s' '%s'", command, pathname);
	}

	/* Count format items (starting with %). */
	s = strdup (command);
	if (s == NULL)
	{
		err (FAILURE, "Function strdup failed in do_command");
	}
	state = s;
	formats = 0;
	token = strsep (&state, "%");
	while (token != NULL)
	{
		formats++;
		token = strsep (&state, "%");
	}
	formats--;
	if (formats < 0)
	{
		err (FAILURE, "Wrong formats in do_command - confused");
	}
	if (formats > 4)
	{
		err (FAILURE, "Too many formats in do_command");
	}

	/* Build command. */
	if ((strlen (command) + formats * (strlen (pathname) + 2) + 3) >
		COMMAND_LENGTH)
	{
		err (FAILURE, "Strings too long for command");
	}
	switch (formats)
	{
	case 0:
		(void) strcpy (cmd, command);
		(void) strcat (cmd, " ");
		(void) strcat (cmd, "'");
		(void) strcat (cmd, pathname);
		(void) strcat (cmd, "'");
		break;
	case 1:
		(void) sprintf (cmd, command, pathname);
		break;
	case 2:
		(void) sprintf (cmd, command, pathname, pathname);
		break;
	case 3:
		(void) sprintf (cmd, command, pathname, pathname, pathname);
		break;
	case 4:
		(void) sprintf (cmd, command, pathname, pathname, pathname, pathname);
		break;
	default:
		err (FAILURE, "Bad formats in do_command - confused");
		break;
	}

	/* Run the command. */
	if (debug > 5)
	{
		msg ("Running '%s'", cmd);
	}
	status = system (cmd);
	if (status != 0)
	{
		err (FAILURE, "do_command failed for file %s", pathname);
	}
}

/* Print help. */

void
print_help (void)
{
	(void) fprintf (stdout, "\
This program is like the find utility, for iRODS.\n\
Processes a directory tree and executes a command for each file/collection.\n\
Usage:\n\
    find [-b batchsize][-c connection][-d level][-h] collection\n\
where\n\
    -h              prints this help\n\
    -C connection   is the connect details for the database. Quoted string.\n\
    -D              Select directories/collections only,\n\
                    files will not be listed.\n\
                    Otherwise it will list directories and files.\n\
                    First the directory and then files in that directory.\n\
    -b batchsize    is the number of rows to process in one go.\n\
                    The default is 1024.\n\
    -c command      is the command to execute for all files/directories.\n\
                    Quoted string.\n\
    -d level        set the debug level, greater for more details.\n\
    -p n            show progress indicator for every n files.\n\
    -q              set quiet.\n\
    -s type         set sort type, 0 for no sort, 1 ascending, 2 descending.\n\
    -v              set verbose.\n\
    collection      is a collection/directory to use as root of the tree.\n\
");
	 exit (FAILURE);
}

/* Main program. */

int
main (int argc, char *argv[])
{

	/* Option string. */
	char *options = "hC:Db:d:c:p:qs:v";

	/* Getopt option. */
	int ch;

	/* Connect string from command line. */
	char *connect_string = "dbname=ICAT user=irods";

	/* Directories only. */
	boolean dirsonly = false;

	/* Batch size, number of rows to process in one go. */
	int batchsize = 1024;

	/* Sort type. */
	int sort = 0;

	/* Command to execute for all files. */
	char *command = "";

	/* Directory name. */
	char *directory = "";

	/* Directory name length. */
	size_t directory_len;

	/* Database connection handle. */
	PGconn *conn;

	/* Postgres exec result. */
	PGresult *res;

	/* Postgres handle for directories. */
	pghandle_t *hd;

	/* Postgres handle for files in a directory. */
	pghandle_t *hf;

	/* Number of database fields. */
	int ncols;

	/* Number of database rows returned. */
	int nrows;

	/* Rows and columns. */
	int i, j;

	/* Number of records seen. */
	long unsigned rno;

	/* Number of directories. */
	long unsigned dno;

	/* Number of files. */
	long unsigned fno;

	/* Collection id. */
	char *coll_id;

	/* Directory name. */
	char *dirname;

	/* File name. */
	char *filename;

	/* Path name. */
	char *pathname;

	/* Total size, all files under the specified directory tree. */
	long unsigned total;

	/* File size. */
	long unsigned filesize;

	/* Get command line switches. */
	ch = getopt (argc, argv, options);
	while (ch != EOF)
	{
		switch (ch)
		{
		case 'h':
			print_help ();
			break;
		case 'C':
			connect_string = optarg;
			break;
		case 'D':
			dirsonly = true;
			break;
		case 'b':
			batchsize = atoi (optarg);
			if (batchsize <= 0)
			{
				err (FAILURE, "Wrong number for batch size");
			}
			break;
		case 'd':
			debug = atoi (optarg);
			if (debug <= 0)
			{
				err (FAILURE, "Wrong number for debug level");
			}
			break;
		case 'c':
			command = optarg;
			break;
		case 's':
			sort = atoi (optarg);
			if (sort < 0)
			{
				err (FAILURE, "Wrong number for sort type");
			}
			break;
		case 'v':
			verbose = 1;
			break;
		case 'p':
			progress = atoi (optarg);
			if (progress <= 0)
			{
				err (FAILURE, "Wrong number for progress");
			}
			break;
		case 'q':
			quiet = true;
			break;
		default:
			err (FAILURE, "Unknown switch %c", ch);
			break;
		}
		ch = getopt (argc, argv, options);
	}

	/* Check for arguments. */
	if (argc < 2)
	{
		err (FAILURE, "iRODS collection must be specified");
	}
	if (optind > argc)
	{
		err (FAILURE, "Wrong optind in getopt processing - confused");
	}

	/* Get directory name (iRODS collection). */
	directory = argv[optind];

	/* Print debug info. */
	if (debug > 5)
	{
		msg ("Connect string is '%s'", connect_string);
		if (dirsonly)
		{
			msg ("Directories only");
		}
		msg ("Batch size is %d", batchsize);
		msg ("Debug level is %d", debug);
		msg ("Command string is '%s'", command);
		msg ("Sort type is %d", sort);
		if (verbose)
		{
			msg ("Verbose is on");
		}
		msg ("Progress indicator is %d", progress);
		if (quiet)
		{
			msg ("Quiet is on");
		}
		msg ("Directory string is '%s'", directory);
	}

	/* Check directory string. */
	directory_len = strlen (directory);
	if (*(directory + directory_len - 1) == SLASH)
	{

		/* The last character is slash. */
		err (FAILURE, "Directory name should not have trailing slash");
	}
	if (! (*directory == SLASH))
	{

		/* The first character is not slash. */
		err (FAILURE, "Directory name should be an absolute pathname");
	}

	/* Connect to database. */
	conn = PQconnectdb (connect_string);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		perr (CANTCONNECT, conn, "Cannot connect as %s",
			connect_string);
	}

	/* Start transaction block. We are only reading which is the default. */
	res = pcmd (conn, "BEGIN");
	PQclear (res);

	/* Full pathname. */
	pathname = (char *) allocate (PATHNAME_LENGTH);

	/* Issue Postgres select for the directory tree. */
	hd = select_directories (conn, sort, batchsize, directory);

	/* Go through the directories. */
	rno = (long unsigned) 0;
	dno = (long unsigned) 0;
	fno = (long unsigned) 0;
	total = (long unsigned) 0;
	fetch (hd);
	rno += (long unsigned) hd->nrows;
	dno += (long unsigned) hd->nrows;
	while (hd->nrows > 0)
	{
		for (i=0; i<(hd->nrows); i++)
		{

			/* Collection internal id and collection name. */
			coll_id = PQgetvalue(hd->res, i, 0);
			dirname = PQgetvalue(hd->res, i, 1);

			if (dirsonly)
			{

				/* Print directory info. */
				if (verbose)
				{
					info ("%s", dirname);
				}
				if (strlen (command) != 0)
				{
					do_command (command, dirname);
				}
			}
			else
			{

				/* Now the files in that directory. */
				hf = select_files (conn, sort, batchsize, coll_id);
				fetch (hf);
				rno += (long unsigned) hf->nrows;
				fno += (long unsigned) hf->nrows;
				while (hf->nrows > 0)
				{
					for (j=0; j<(hf->nrows); j++)
					{

						/* File name. */
						filename = PQgetvalue(hf->res, j, 1);

						/* File size. */
						filesize = (long unsigned)
							atol (PQgetvalue(hf->res, j, 0));
						total += filesize;

						/* Print file info. */
						if ((strlen (dirname) + strlen (filename) + 2) >
							PATHNAME_LENGTH)
						{
							err (FAILURE, "Pathname too long");
						}
						(void) strcpy (pathname, dirname);
						(void) strcat (pathname, "/");
						(void) strcat (pathname, filename);
						if (verbose)
						{
							info ("%s", pathname);
						}
						if (strlen (command) != 0)
						{
							do_command (command, pathname);
						}
					}

					/* Next batch of files. */
					fetch (hf);
					rno += (long unsigned) hf->nrows;
					fno += (long unsigned) hf->nrows;
					show_progress (rno);
				}
				closecursor (hf);
			}
		}
		fetch (hd);
		rno += (long unsigned) hd->nrows;
		dno += (long unsigned) hd->nrows;
	}
	closecursor (hd);
	free (pathname);

	/* Finish. */
	if (verbose > 0)
	{

		/* Print summary. */
		msg ("%24lu records seen", rno);
		msg ("%24lu directories", dno);
		msg ("%24lu files", fno);
		msg ("%24lu bytes grand total", total);
	}
	res = pcmd (conn, "END");
	PQclear (res);
	PQfinish (conn);
	exit (SUCCESS);
}

/* End of file IFIND.C */
