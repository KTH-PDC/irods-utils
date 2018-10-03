
/* IFIND.C executes a script/program for every file in a directory tree. */

/* System include files. */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <locale.h>
#include <xlocale.h>
#include <regex.h>
#include <math.h>
#include <limits.h>

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

/* Slash character. */
#define SLASH '/'

/* Exit codes. */
#define PGRESEXEC	((int) 1)		/* Postgres execute error */
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

/* Verbose. */
static int verbose = false;

/* Progress indicator. */
static int progress = 0;

/* SQL statement. */
static char *sqlstmt = NULL;

/* Number of SQL statements executed. */
static long unsigned nsqlstmt = 0;

/* Regexp. */
static char *regexp = NULL;

/* Regexp compiled. */
static regex_t *rxc = NULL;

/* Regexp substitution. */
static char *regexpsubst = NULL;

/* Replica number. Signal no preferred replica as default. */
static char *replica = NULL;

/* Resource. */
static char *resource = NULL;

/* Persistent retry. */
static int retry = false;

/* Print summary. */
static int summary = false;

/* Test, print command string only. */
static int test = false;

/* Continue when command fails. */
static int force = false;

/* Check path length. */
static int check_length = 0;

/* Will quit after this many failures. */
static int max_retry_failures = 32768;

/* Number of retries in one go. */
static int max_retries = 3;

/* Delay after a retry. */
static int delay_retry = 59;

/* Counter for retries. Will count down from maximum value. */
static int retry_failures = 0;

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

/* Progress message (no new line). */

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

/* POSIX regexp compare to compiled re string. */

static int
rmatch (char *s)
{
	int r;

	/* Match using global rxc. */
	r = regexec (rxc, s, 0, NULL, 0);
	if (r == 0)
	{

		/* Match. */
		return (true);
	}
	else if (r == REG_NOMATCH)
	{

		/* Did not match. */
		return (false);
	}
	else
	{

		/* Other error. */
		err (FAILURE, "Function regexec failed with %d", r);
	}

	/* Never reached. */
	return (false);
}

/* Regexp substitute. Overwrites result with substituted. */

static void
rsubs (char *result, char *s, char *rs)
{
	char *op;
	char *ip;
	size_t slen;
	size_t rlen;
	size_t len;
	size_t nmatch;
	regmatch_t pmatch[1];
	int status;
	regoff_t rstart;
	regoff_t rend;

	/* Assign input and output pointers. */
	op = result;
	ip = s;

	/* Length for orgininal string and substitution. */
	slen = strlen (s);
	rlen = strlen (rs);

	/* No parenthesized subexpressions, we got only one element. */
	nmatch = (size_t) 1;
	status = regexec (rxc, s, nmatch, pmatch, 0);
	if (status == 0)
	{

		/* Start and end offsets, with checks. */
		rstart = pmatch[0].rm_so;
		if (rstart == (regoff_t) -1)
		{
			err (FAILURE, "No match, rstart is -1 - confused");
		}
		rend = pmatch[0].rm_eo;

		/* Check destination size. */
		if ((slen - ((size_t) rend - (size_t) rstart) + rlen + 1) >=
			PATHNAME_LENGTH)
		{
			err (FAILURE, "Does not fit substituted '%s' - confused", s);
		}

		/* We got start and end offset, debug print. */
		if (debug > 10)
		{
			msg ("%d %d '%s' '%s' '%s'", rstart, rend, result, s, rs);
		}

		/* Copy first part of the string. */
		len = (size_t) rstart;
		memcpy (op, ip, len);
		ip += len;
		op += len;

		/* Second part is the substitution. */
		len = rlen;
		memcpy (op, rs, len);
		ip += (size_t) rend - (size_t) rstart;
		op += len;

		/* Third part is the rest. */
		len = slen - ((size_t) (rend));
		memcpy (op, ip, len);

		/* Print substituted string. */
		if (debug > 10)
		{
			msg ("%d %d '%s' '%s' '%s'", rstart, rend, result, s, rs);
		}
	}
	else
	{

		/* We've had a match already when here so this is impossible. */
		err (FAILURE, "Function regexec returned nonzero %d - confused",
			status);
	}
}

/* Print pathname. */

static void
infopath (char *pathname)
{

	/* This is pathname with substitutions. */
	char r[PATHNAME_LENGTH];

	if (regexp != NULL)
	{

		/* Print when there is a match. */
		if (rmatch (pathname))
		{
			if (regexpsubst != NULL)
			{

				/* All EOS. */
				memset (r, 0, (size_t) PATHNAME_LENGTH);

				/* Do substitutions. */
				rsubs (r, pathname, regexpsubst);

				/* Overwrite pathname. */
				(void) strncpy (pathname, r, PATHNAME_LENGTH);

				/* Print eventually the substituted pathname. */
				info ("%s", pathname);
			}
			else
			{

				/* No substitution, regexp match, print name. */
				info ("%s", pathname);
			}
		}
	}
	else if (check_length > 0)
	{

		/* If length check required print the name if too long. */
		if (strlen (pathname) > check_length)
		{
			info ("%s", pathname);
		}
	}
	else
	{

		/* No regexp print always. */
		info ("%s", pathname);
	}
}

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

/* Our database connection info block. */
typedef struct dbc {

	/* Database connection handle. */
	PGconn *conn;

	/* Postgres exec result. */
	PGresult *res;

	/* Postgres handle for directories. */
	pghandle_t *hd;

	/* Postgres handle for files in a directory. */
	pghandle_t *hf;

	/* Start and end time. */
	time_t starttime;
	time_t endtime;

	/* Number of records seen. */
	long long unsigned rno;

	/* Number of directories. */
	long long unsigned dno;

	/* Number of files. */
	long long unsigned fno;

	/* Not UTF counted. */
	long long unsigned nutfno;

	/* Number of fetches. */
	long long unsigned fetches;

	/* Grand total size, all files under the specified directory tree. */
	long long unsigned total;

	/* Last command executed. */
	char *last_command;

	/* Last path visited. */
	char *last_path;
} dbc_t;

/* Global variable for database info block. */
dbc_t *dbc;

/* Create database connection structure. */

static dbc_t *
create_dbc (void)
{
	dbc_t *r;

	r = new (dbc_t);
	r->conn = NULL;
	r->res = NULL;
	r->hd = NULL;
	r->hf = NULL;
	r->starttime = (time_t) 0;
	r->endtime = (time_t) 0;
	r->rno = (long long unsigned) 0;
	r->dno = (long long unsigned) 0;
	r->fno = (long long unsigned) 0;
	r->nutfno = (long long unsigned) 0;
	r->fetches = (long long unsigned) 0;
	r->total = (long long unsigned) 0;
	r->last_command = (char *) allocate (COMMAND_LENGTH);
	(void) strcpy (r->last_command, "none");
	r->last_path = (char *) allocate (PATHNAME_LENGTH);
	(void) strcpy (r->last_path, "none");
	return (r);
}

/* Return size string in SI units to print. */

static char *
printsize (long long unsigned total)
{

	/* Various sizes. */
	long long unsigned kib = 1024;
	long long unsigned mib = 1024 * kib;
	long long unsigned gib = 1024 * mib;
	long long unsigned tib = 1024 * gib;
	long long unsigned pib = 1024 * tib;
	long long unsigned eib = 1024 * pib;
	long long unsigned zib = 1024 * eib;
	long long unsigned yib = 1024 * zib;

	/* Converted string with leeway. */
	char cvs[24 + 4 + 1 + 64];

	/* String to be returned. */
	char *r;

	/* With SI unit prefixes. */
	if (total < kib)
	{
		(void) sprintf (cvs, "%24llu B", total);
	}
	else if (total >= kib && total < mib)
	{
		(void) sprintf (cvs, "%24llu KiB", total / kib);
	}
	else if (total >= mib && total < gib)
	{
		(void) sprintf (cvs, "%24llu MiB", total / mib);
	}
	else if (total >= gib && total < tib)
	{
		(void) sprintf (cvs, "%24llu GiB", total / gib);
	}
	else if (total >= tib && total < pib)
	{
		(void) sprintf (cvs, "%24llu TiB", total / tib);
	}
	else if (total >= pib && total < eib)
	{
		(void) sprintf (cvs, "%24llu PiB", total / pib);
	}
	else if (total >= eib && total < zib)
	{
		(void) sprintf (cvs, "%24llu EiB", total / eib);
	}
	else if (total >= zib && total < yib)
	{
		(void) sprintf (cvs, "%24llu ZiB", total / zib);
	}
	else
	{
		(void) sprintf (cvs, "%24llu YiB", total / yib);
	}
	r = strdup (cvs);
	return (r);
}

/* Print summary. */

static void
print_summary (dbc_t *d)
{

	/* Bandwidth, bit / s. */
	long long unsigned totalbps;

	/* Total size string. */
	char *totalsize;

	/* Total speed string. */
	char *totalspeed;

	/* Duration time in seconds. */
	long long unsigned duration;

	msg ("%24llu records seen", d->rno);
	msg ("%24llu directories", d->dno);
	msg ("%24llu files", d->fno);
	msg ("%24llu bytes grand total", d->total);
	totalsize = printsize (d->total);
	msg ("%24s grand total", totalsize);
	if (d->nutfno > 0)
	{
		msg ("%24llu malformed", d->nutfno);
	}
	free (totalsize);
	duration = (long long unsigned) (d->endtime - d->starttime);
	if (duration == 0)
	{
		msg ("%24s %s", "n/a", "Finished in less than a second");
	}
	else
	{
		msg ("%24llu seconds duration", duration);
		totalbps = d->total / duration;
		msg ("%24llu bytes/s", (d->total / duration));
		totalspeed = printsize (totalbps);
		msg ("%24s / second", totalspeed);
		free (totalspeed);
	}
}

/* Maximum length of SQL statement. */
#define MAX_SQL_STMT ((int) 65535)

/* Execute SQL statement for an object. */

static void
execute_sqlstmt (PGconn *conn, char *sql, long long unsigned id, char *path)
{
	int status;

	/* Postgres exec result. */
	PGresult *res;

	/* Statement length. */
	size_t s;

	/* Postgresql statement. */
	char stmt[MAX_SQL_STMT];

	/* The SQL string should accomodate the ID and a bit more. */
	s = (size_t) MAX_SQL_STMT - (size_t) (log10 (ULLONG_MAX) + 2)
		- (size_t) 16;

	/* It should have proper format string included. */
	if (strstr (sql, "%llu") == NULL)
	{
		err (FAILURE, "SQL statement string does not have %llu for id");
	}

	/* Edit id into the string. */
	status = snprintf (stmt, s, sql, id);
	if (status > (int) s)
	{

		/* Fail out if too long and truncated. */
		err (FAILURE, "SQL statement string %s too long", stmt);
	}
	if (status < 0)
	{
		err (FAILURE, "Function snprintf failed %d - confused", status);
	}

	/* We got the SQL statement, execute. */
	if (debug > 5)
	{
		msg ("SQL '%s' for %s", stmt, path);
	}
	if (test)
	{
		msg ("%s", stmt);
	}
	else
	{

		/* Execute SQL command. */
		res = pcmd (conn, stmt);
		PQclear (res);
		nsqlstmt++;
	}
}

/* Issue select for directories. */

static pghandle_t *
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
	else if (sorted == 3)
	{

		/* Directories are always unique. */
		strcat (colls_cmd,  " ORDER BY coll_name ASC");
	}
	else if (sorted == 4)
	{

		/* Directories are always unique. */
		strcat (colls_cmd,  " ORDER BY coll_name DESC");
	}
	else
	{
		err (FAILURE, "Wrong sort option %d", sorted);
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

static pghandle_t *
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
	if (sorted == 3 || sorted == 4)
	{

		/* Unique file names. */
		files_select =
			"DECLARE d CURSOR FOR SELECT DISTINCT ON (data_name) \
data_size,data_name \
FROM r_data_main WHERE coll_id=%s";
	}
	else
	{

		/* Default case, show all matches. There will be replicas. */
		files_select = "DECLARE d CURSOR FOR SELECT \
data_id,data_size,data_name \
FROM r_data_main WHERE coll_id=%s";
	}
	files_len = (size_t) (strlen (files_select) +
		strlen(directory));

	/* Create sprintf target, should be long enough. */
	files_cmd = (char *) allocate (strlen (files_select) + files_len +
		1024);

	/* Create SQL statement. */
	(void) sprintf (files_cmd, files_select, directory);

	/* Add resource clause when needed. */
	if (resource != NULL)
	{
		strcat (files_cmd, " AND RESC_NAME = '");
		strcat (files_cmd, resource);
		strcat (files_cmd, "'");
	}

	/* Add replica clause if specified. The replica number is actually
	   a string! */
	if (replica != NULL)
	{

		/* Replica number was specified. */
		strcat (files_cmd, " AND DATA_REPL_NUM = '");
		strcat (files_cmd, replica);
		strcat (files_cmd, "'");
	}

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
	else if (sorted == 3)
	{
		strcat (files_cmd,  " ORDER BY data_name ASC");
	}
	else if (sorted == 4)
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

static void
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

static void
closecursor (pghandle_t *h)
{

	/* Issue the close cursor. */
	h->res = pcmd (h->conn, h->close_cmd);

	/* Free memory. */
	PQclear (h->res);

	/* Free more memory. */
	h->conn = NULL;
	h->res = NULL;
	free (h->select_cmd);
	free (h->fetch_cmd);
	free (h->close_cmd);
	h->select_cmd = NULL;
	h->fetch_cmd = NULL;
	h->close_cmd = NULL;
	h->nrows = 0;
	h->nfields = 0;
	free (h);
}

/* Show progress */

static void
show_progress (long long unsigned fetches)
{
	if (progress > 0)
	{

		/* We got the number of fetches, one fetch should get batchsize
		   records, sometimes less. */
		if ((fetches % ((long long unsigned) progress)) ==
			(long long unsigned) 0)
		{
			pmsg (".");
		}
	}
}

/* Check if pathname is a UTF-8 string. */

static int
is_utf (char *pathname)
{
	size_t l;

	l = mbstowcs (NULL, pathname, 0);
	if (l == (size_t) -1)
	{
		return (false);
	}
	else
	{
		return (true);
	}
}

/* Maximum number of tasks. */
#define MAX_TASKS ((int) 64)

/* Task descriptor. */
typedef struct
{

	/* Task number. */
	int no;

	/* Task PID. */
	pid_t pid;

	/* Number of commands to execute. */
	int ncmd;

	/* Next command slot. */
	int nextcmd;

	/* Array of commands. */
	char **cmds;
} task_t;

/* Work descriptor. */
typedef struct
{

	/* Number of tasks. */
	int ntasks;

	/* Next task slot. */
	int nexttask;

	/* Array of pointers to tasks. */
	task_t **tasks;

	/* Running indicator. */
	int running;
} work_t;

/* Global variable, work descriptor with tasks array. */
work_t *work = NULL;

/* Initialize task descriptor with n tasks, m commands each. */

static task_t **
init_tasks (int n, int m)
{
	task_t **r;
	int i;
	int j;

	/* Check. */
	if (n > MAX_TASKS)
	{
		err (FAILURE, "Too many tasks (%d) maximum %d",
			n, MAX_TASKS);
	}

	/* Create array of pointers to tasks. */
	r = (task_t **) allocate (n * sizeof (task_t *));

	/* Allocate space for the commands array in all tasks. */
	for (i=0; i<n; i++)
	{
		r[i] = (task_t *) allocate (sizeof (task_t));
		r[i]->no = i;
		r[i]->pid = 0;
		r[i]->ncmd = m;
		r[i]->nextcmd = 0;
		r[i]->cmds = (char **) allocate (m * sizeof (char *));
		for (j=0; j<m; j++)
		{
			r[i]->cmds[j] = (char *) allocate (COMMAND_LENGTH);
		}
	}
	return (r);
}

/* Create work descriptor. */

static work_t *
create_work (int n, int m)
{
	work_t *r;

	r = new (work_t);
	r->ntasks = n;
	r->nexttask = 0;
	r->tasks = init_tasks (n, m);
	r->running = false;
	return (r);
}

/* Cleaning up. Refers global variable db. */

static void
cleanup (void)
{
	PGresult *res;

	/* Update end time. */
	dbc->endtime = time (NULL);
	if (dbc->endtime == (time_t) -1)
	{
		err (FAILURE, "Error getting end time");
	}

	/* Roll back end finish. */
	closecursor (dbc->hf);
	closecursor (dbc->hd);
	res = PQexec (dbc->conn, "ROLLBACK");
	PQclear (res);
	PQfinish (dbc->conn);
}

/* Signal handler. */

static void
signal_handler (int signo)
{
	if (signo == SIGHUP || signo == SIGINT ||
		signo == SIGQUIT || signo == SIGTERM)
	{

		/* Interrupt received, cleanup and exit. */
		(void) fprintf (stderr, "Interrupted, cleaning up and exiting\n");
		(void) fprintf (stderr, "Last path was: '%s'\n", dbc->last_path);
		(void) fprintf (stderr, "Last command was: '%s'\n", dbc->last_command);
		cleanup ();
		print_summary (dbc);
		exit (FAILURE);
	}
	else
	{
		(void) fprintf (stderr, "Interrupted, signal %d - confused\n", signo);
		(void) fflush (stderr);
	}
}

/* Establish signal handler. */

static void
siga (int s, struct sigaction *sa)
{
	int status;

	sa->sa_handler = signal_handler;
	status = sigemptyset (&sa->sa_mask);
	if (status == -1)
	{
		err (FAILURE, "Error calling sigemptyset - confused");
	}
	sa->sa_flags = 0;
	status = sigaction (s, sa, NULL);
	if (status == -1)
	{
		err (FAILURE, "Error calling sigaction - confused");
	}
}

/* Build the command using command string and pathname. */

static void
build_command (char *cmd, char *cmds, char *pathname)
{
	char *s;
	char *state;
	char *token;
	int formats;

	/* Null command string. */
	if (cmds == NULL)
	{
		err (FAILURE, "Command string is null");
	}

	/* Check. */
	if (strlen (cmds) == (size_t) 0)
	{
		err (FAILURE, "Empty command string in build_command");
	}
	if (index (pathname, 0x027) != NULL)
	{
		err (FAILURE, "Single quote detected in %s", pathname);
	}

	/* Enter command function. */
	if (debug > 10)
	{
		msg ("Build command '%s' '%s'", cmds, pathname);
	}

	/* Count format items (starting with %). */
	s = strdup (cmds);
	if (s == NULL)
	{
		err (FAILURE, "Function strdup failed in build_command");
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
		err (FAILURE, "Wrong formats in build_command - confused");
	}
	if (formats > 4)
	{
		err (FAILURE, "Too many formats in build_command");
	}
	free (s);

	/* Build command. */
	if ((strlen (cmds) + formats * (strlen (pathname) + 2) + 3) >
		COMMAND_LENGTH)
	{
		err (FAILURE, "Strings too long for command");
	}
	switch (formats)
	{
	case 0:
		(void) strcpy (cmd, cmds);
		(void) strcat (cmd, " ");
		(void) strcat (cmd, "'");
		(void) strcat (cmd, pathname);
		(void) strcat (cmd, "'");
		break;
	case 1:
		(void) sprintf (cmd, cmds, pathname);
		break;
	case 2:
		(void) sprintf (cmd, cmds, pathname, pathname);
		break;
	case 3:
		(void) sprintf (cmd, cmds, pathname, pathname, pathname);
		break;
	case 4:
		(void) sprintf (cmd, cmds, pathname, pathname, pathname, pathname);
		break;
	default:
		err (FAILURE, "Bad formats in build_command - confused");
		break;
	}
}

/* Run command with retry. */

int
run_command (int retries, int period, char *cmd)
{
	int status;
	int nretries;
	int unslept;

	if (retries <= 0)
	{

		/* Won't retry. Fail out when there was an error. */
		status = system (cmd);
		if (status == -1)
		{

			/* The system library function failed. */
			err (FAILURE, "There was a system error running '%s'", cmd);
		}
		else
		{
			if (status != 0)
			{

				/* Non-zero returned from the command.
				   Don't quit when forced. */
				if (! force)
				{

					/* Abort will never return. */
					err (FAILURE, "Error %d running '%s'", status, cmd);
				}
				return (status);
			}
			else
			{

				/* No error, status is 0, finish here. */
				return (0);
			}
		}
	}
	else
	{

		/* Retry, there was an error. */
		nretries = retries;
		while (nretries > 0 && status != 0)
		{

			/* Wait. */
			unslept = sleep (period);
			if (unslept != 0)
			{
				err (FAILURE, "There was an error sleeping for '%s'", cmd);
			}

			/* Retry. */
			status = system (cmd);
			if (status == -1)
			{
				err (FAILURE, "There was a system error running '%s'", cmd);
			}
			else
			{
				if (status != 0)
				{

					/* Non-zero returned from the command. */
					msg ("Error %d retrying '%s'", status, cmd);
					if (retry_failures >= max_retry_failures)
					{

						/* Reached the limit. */
						err (FAILURE,
							"There were more than %d command retries - abort",
							max_retry_failures);
					}
					retry_failures++;
				}

				/* We got zero status. */
				return (0);
			}
		}

		/* Still failing after so many retries. */
		msg ("Command '%s' still failing after %d retries", cmd, retries);
	}

	/* Finish, with or without retries with the status of the last run. */
	return (status);
}

/* Do command. */

static int
do_command (char *cmd)
{
	int status;

	if (strlen (cmd) == 0)
	{
		err (FAILURE, "Command is the empty string", cmd);
	}
	if (debug > 10)
	{
		msg ("Running command '%s'", cmd);
	}
	if (test)
	{
		msg ("%s", cmd);
	}
	else
	{

		/* Take a note. */
		(void) strncpy (dbc->last_command, cmd, COMMAND_LENGTH);

		/* Run the command with shell. */
		if (retry)
		{

			/* Retry three times after waiting for 59 seconds. */
			status = run_command (3, 59, cmd);
		}
		else
		{
			status = run_command (0, 0, cmd);
		}
		if (status == -1)
		{
			err (FAILURE, "There was an error running '%s'", cmd);
		}
		if (status != 0)
		{
			if (! force)
			{
				(void) fprintf (stderr, "Command failed, status %d\n", status);
				(void) fprintf (stderr, "Command was: '%s'\n", cmd);
				(void) fflush (stderr);
				cleanup ();
				print_summary (dbc);
				err (FAILURE, "Command returned nonzero status %d", status);
			}
			else
			{

				/* Call system ignores SIGINT and SIGQUIT so
				   extra magic needed. */
				if (WIFSIGNALED (status))
				{
					if (WTERMSIG (status) == SIGHUP ||
						WTERMSIG (status) == SIGINT ||
						WTERMSIG (status) == SIGQUIT ||
						WTERMSIG (status) == SIGTERM)
					{

						/* There was an interrupt. */
						msg ("Interrupted %s", cmd);
						cleanup ();
						print_summary (dbc);
						err (FAILURE, "Interrupted with %d", status);
					}
				}
				else
				{
					(void) fprintf (stderr, "Error %d for '%s'\n", status, cmd);
					(void) fflush (stderr);
				}
			}
		}
	}
	return (status);
}

/* Execute function in parallel. */

static void
parallel (work_t *w, int (*f)(work_t *w, int taskid))
{
	task_t **tasks;
	int n;
	int i;
	pid_t pid;
	pid_t cpid;
	int cstatus;
	int exited;
	int wait_pid;
	int wait_status;

	/* Get work data. */
	tasks = w->tasks;
	n = w->ntasks;

	/* Check. */
	if (n > MAX_TASKS || n <= 0)
	{
		err (FAILURE, "Wrong number of tasks (%d), should be 0 < n <= %d",
			n, MAX_TASKS);
	}

	/* Fork n subprocesses. */
	for (i=0; i<n; i++)
	{
		pid = fork ();
		if (pid < (pid_t) 0)
		{

			/* Detailed reporting. */
			(void) fprintf (stderr, "Forking %d tasks\n", n);
			(void) fprintf (stderr, "Fork failed with %d\n", (int) pid);
			(void) fprintf (stderr, "Corresponds to: '%s'\n", strerror (errno));
			(void) fflush (stderr);
			err (FAILURE, "Cannot fork task %d", i);
		}
		if (pid == (pid_t) 0)
		{
			/* Child. */
			cpid = getpid ();
			tasks[i]->pid = cpid;

			/* Run the function. */
			cstatus = f (w, i);

			/* Exit child. */
			exit (cstatus);
		}
		else
		{

			/* Parent. */
			;
		}
	}

	/* Wait for all of them to exit. */
	exited = 0;
	while (exited < n)
	{
		wait_pid = wait (&wait_status);
		if (wait_pid < 0)
		{
			err (FAILURE, "Error waiting");
		}
		exited++;
	}
}

/* Run the queue. */

static int
run_queue (work_t *w, int taskid)
{
	task_t *t;
	int i;
	int n;
	int status;

	/* Taskid is the same as the index. */
	t = w->tasks[taskid];

	/* Process the task. */
	status = 0;
	n = t->nextcmd;
	if (n < 0)
	{

		/* This in fact means that nextcmd was 0 at this point, i.e. no task. */
		if (debug > 5)
		{
			msg ("Running the queue as task %d, no cmds", taskid);
		}
	}
	else
	{
		if (debug > 5)
		{
			msg ("Running the queue as task %d, %d cmds", taskid, n);
		}

		/* n is the number of commands - 1, that is the last index, 0..n-1. */
		for (i=0; i<n; i++)
		{

			/* Just execute the command string. */
			status = do_command (t->cmds[i]);
		}
	}

	/* Return with the status of the last command. */
	return (status);
}

/* Queue command for later parallel execution. */

static void
queue_command (work_t *w, char *cs)
{
	task_t *t;
	int i;

	/* Fill in the next available slot, if any. */
	t = w->tasks[w->nexttask];
	if (t->nextcmd < t->ncmd)
	{

		/* Still have empty slot with current task, insert command. */
		if (debug > 5)
		{
			msg ("Filling task %d slot %d",
				w->nexttask, t->nextcmd);
		}
		strncpy (t->cmds[t->nextcmd], cs, COMMAND_LENGTH);
		t->nextcmd++;
	}
	else
	{

		/* Current task is full, need to move on to the next one. */
		w->nexttask++;
		if (w->nexttask < w->ntasks)
		{

			/* We are on the next empty task. */
			t = w->tasks[w->nexttask];
			if (t->nextcmd != 0)
			{

				/* Next task is not empty, we've got an issue. */
				err (FAILURE,
					"Moving on to next task but nonempty slots - confused");
			}

			/* Insert command as first for this task. */
			if (debug > 5)
			{
				msg ("Filling task %d slot %d", w->nexttask, t->nextcmd);
			}
			strncpy (t->cmds[t->nextcmd], cs, COMMAND_LENGTH);
			t->nextcmd++;
		}
		else
		{
			/* All tasks full so run the queue. */
			if (debug > 5)
			{
				msg ("Start running the queue with %d tasks", w->ntasks);
			}
			w->running = true;
			parallel (w, run_queue);

			/* Finished running the queue, clean up. */
			if (debug > 5)
			{
				msg ("Clean up queue");
			}
			w->running = false;
			w->nexttask = 0;
			for (i=0; i<w->ntasks; i++)
			{
				w->tasks[i]->pid = 0;
				w->tasks[i]->nextcmd = 0;
			}
		}
	}
}

/* Flush the queue. */

static void
flush_queue (work_t *w)
{
	int i;

	/* The queue is to be flushed when there is stuff in it. */
	if ((w->tasks[0]->nextcmd > 0) && (! w->running))
	{
		/* At least the first task has stuff and not running. */
		if (debug > 5)
		{
			msg ("Flushing the queue");
		}
		w->running = true;
		parallel (w, run_queue);
		w->running = false;
		w->nexttask = 0;
		for (i=0; i<w->ntasks; i++)
		{
			w->tasks[i]->pid = 0;
			w->tasks[i]->nextcmd = 0;
		}
	}
}

/* Execute command for a pathname. */

static void
execute (int ntasks, char *command, char *path)
{
	char cs[COMMAND_LENGTH];
	int status;

	/* Skip when the path does not match regexp. */
	if (regexp != NULL)
	{
		if (! rmatch (path))
		{
			return;
		}
	}

	/* NOP when no command was specified. */
	if (command != NULL)
	{
		build_command (cs, command, path);
		if (ntasks > 0)
		{
			queue_command (work, cs);
		}
		else
		{
			status = do_command (cs);
		}
	}
	else
	{

		/* No command specified. Also we should run only one
		   task (garbled output). */
		if (ntasks > 0 )
		{
			err (FAILURE, "Cannot multitask when no command was specified");
		}

		/* And just fail out anyway. */
		err (FAILURE, "No command to execute");
	}
}

/* Print help. */

static void
print_help (void)
{
	(void) fprintf (stdout, "\
This program is like the find utility, for iRODS.\n\
Processes a directory tree and executes a command for each file/collection.\n\
Usage:\n\
    find [-h][-C connection][-D][-E resource][-I][-Q sql][R n,w,m]\n\
        [-S][-X regexp][-Y subst]\n\
        [-b batchsize][-c command][-d level][-f][-l n][-n n]\n\
        [-p n][-q][-r n][-s type][-t][-u locale][-v]\n\
        collection\n\
where\n\
    -h              prints this help\n\
    -C connection   is the connect details for the database. Quoted string.\n\
                    The default is 'dbname=ICAT user=irods'.\n\
    -D              Select directories/collections only.\n\
                    In this case files will not be listed.\n\
                    The default is to list files.\n\
    -E resource     restrict to this resource.\n\
    -I              also print file IDs.\n\
    -Q sql          execute SLQ command with object id.\n\
    -R n,w,m        retry failed command n times after waiting for w seconds,\n\
                    allow m retries all in all\n\
    -S              print summary.\n\
    -X regexp       Match regexp.\n\
    -Y substitute   Substitute matching regexp with this.\n\
    -b batchsize    is the number of rows to process in one go.\n\
                    The default is 1024.\n\
    -c command      is the command to execute for all files/directories.\n\
                    Quoted string. The default is to print the pathname.\n\
    -d level        set the debug level, greater for more details.\n\
    -f              force, continue when the command returns non-zero status.\n\
    -l length       check if any file pathnames longer then specified.\n\
    -n n            number of parallel worker tasks.\n\
    -p n            show progress indicator for every n files.\n\
    -q              set quiet.\n\
    -r n            replica number, the default is all replicas\n\
    -s type         set sort type, 0 for no sort, 1 ascending, 2 descending.\n\
                    3 ascending unique, 4 descending unique.\n\
                    The default is not to sort.\n\
    -t              test, print command string.\n\
    -u locale       check pathname according to specified locale.\n\
    -v              set verbose.\n\
    collection      is a collection/directory to use as root of the tree.\n\
");
	 exit (FAILURE);
}

/* Main program. */

int
main (int argc, char *argv[])
{

	/* Status code. */
	int status;

	/* Option string. */
	char *options = "hC:DE:IQ:R:SX:Y:b:c:d:fl:n:p:qr:s:tu:v";

	/* Getopt option. */
	int ch;

	/* Connect string from command line. */
	char *connect_string = "dbname=ICAT user=irods";

	/* Directories only. */
	boolean dirsonly = false;

	/* Print file id with the name. */
	boolean printid = false;

	/* Batch size, number of rows to process in one go. */
	int batchsize = 1024;

	/* Sort type. */
	int sort = 0;

	/* State from strtok call. */
	char *state;

	/* Retry, delay, max retry. */
	int rtr;
	int dly;
	int mrt;

	/* Command to execute for all files. */
	char *command = NULL;

	/* UTF-8 checker locale. */
	char *utf = NULL;

	/* Number of tasks, 0 means paralellism disabled. */
	int ntasks = 0;

	/* Old locale from setlocale. */
	char *oldlocale;

	/* Directory name. */
	char *directory = "";

	/* Directory name length. */
	size_t directory_len;

	/* Signal action block to sigaction. */
	struct sigaction sig;

	/* Database connection handle. */
	PGconn *conn;

	/* Postgres exec result. */
	PGresult *res;

	/* Postgres handle for directories. */
	pghandle_t *hd;

	/* Postgres handle for files in a directory. */
	pghandle_t *hf;

	/* Rows and columns. */
	int i, j;

	/* Collection id. */
	char *coll_id;

	/* Directory name. */
	char *dirname;

	/* File name. */
	char *filename;

	/* File size. */
	long long unsigned filesize;

	/* File id (column dat_id in r_data_main). */
	long long unsigned fileid;

	/* Path name. */
	char *pathname;

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
		case 'E':
			resource = optarg;
			break;
		case 'I':
			printid = true;
			break;
		case 'Q':
			sqlstmt = optarg;
			break;
		case 'R':
			retry = true;

			/* Get three numbers, separated by comma. */
			rtr = atoi (strtok_r (optarg, ",", &state));
			dly = atoi (strtok_r (NULL, ",", &state));
			mrt = atoi (strtok_r (NULL, ",", &state));
			if (rtr <= 0 || dly <= 0 || mrt <= 0)
			{
				err (FAILURE, "Wrong specification for retries");
			}

			/* Maximum retries in one go, delay in seconds and
			   all in all number of retries allowed. */
			max_retries = rtr;
			delay_retry = dly;
			max_retry_failures = mrt;
			break;
		case 'S':
			summary = true;
			break;
		case 'X':
			regexp = optarg;

			/* Compile (and check) regular expression into global. */
			rxc = new (regex_t);
			status = regcomp (rxc, regexp, REG_EXTENDED);
			if (status != 0)
			{
				err (FAILURE, "Wrong POSIX regular expression '%s'", regexp);
			}
			if (rxc->re_nsub != (size_t) 0)
			{
				err (FAILURE, "Cannot do parenthesized subexpressions %s",
					regexp);
			}
			break;
		case 'Y':

			/* Substitution is specified with regexp. */
			regexpsubst = optarg;
			if (regexp == NULL)
			{

				/* Bail out if no regexp. */
				err (FAILURE, "Need to specify -X regexp with -Y");
			}
			break;
		case 'b':
			batchsize = atoi (optarg);
			if (batchsize <= 0)
			{
				err (FAILURE, "Wrong number for batch size");
			}
			break;
		case 'c':
			command = optarg;
			if (command == NULL)
			{
				err (FAILURE, "No argument for command - confused");
			}
			if (strlen (command) == 0)
			{
				err (FAILURE, "Wrong argument for command");
			}
			break;
		case 'd':
			debug = atoi (optarg);
			if (debug <= 0)
			{
				err (FAILURE, "Wrong number for debug level");
			}
			break;
		case 'f':
			force = true;
			break;
		case 'l':
			check_length = atoi (optarg);
			if (check_length <= 0)
			{
				err (FAILURE, "Wrong number for pathname length check");
			}
			break;
		case 'n':
			ntasks = atoi (optarg);
			if (ntasks <= 0)
			{
				err (FAILURE, "Wrong number for number of workers");
			}
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
		case 'r':
			replica = optarg;
			if (strtol (replica, (char **) NULL, 10) < 0)
			{
				err (FAILURE, "Wrong number for replica");
			}
			if (errno == EINVAL || errno == ERANGE)
			{
				err (FAILURE, "Conversion error for replica number");
			}
			break;
		case 's':
			sort = atoi (optarg);
			if (sort < 0)
			{
				err (FAILURE, "Wrong number for sort type");
			}
			break;
		case 't':
			test = true;
			break;
		case 'u':
			utf = optarg;
			if (strlen (utf) > (size_t) 0)
			{
				oldlocale = setlocale (LC_ALL, utf);
				if (oldlocale == NULL)
				{
					err (FAILURE, "Invalid locale name %s", utf);
				}
			}
			break;
		case 'v':
			verbose = true;
			break;
		case '?':
			err (FAILURE, "Unknown switch");
			break;
		default:
			err (FAILURE, "Unknown switch %c", ch);
			break;
		}
		ch = getopt (argc, argv, options);
	}

	/* Checking switches. */
	if (check_length > 0 && regexp != NULL)
	{
		err (FAILURE, "Cannot specify both regexp and length check");
	}
	if (force && retry)
	{
		err (FAILURE, "Do not specify both force and retry");
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
	if (directory == NULL)
	{
		err (FAILURE, "iRODS collection is NULL - confused");
	}

	/* Print debug info. */
	if (debug > 5)
	{
		msg ("Connect string is '%s'", connect_string);
		if (dirsonly)
		{
			msg ("Directories only");
		}
		if (summary)
		{
			msg ("Summary requested");
		}
		msg ("Batch size is %d", batchsize);
		if (command != NULL)
		{
			msg ("Command string is '%s'", command);
		}
		if (debug > 0)
		{
			msg ("Debug level is %d", debug);
		}
		if (force)
		{
			msg ("Ignore errors when running commands");
		}
		if (ntasks > 0)
		{
			msg ("Number of worker tasks is %d", ntasks);
		}
		msg ("Sort type is %d", sort);
		if (verbose)
		{
			msg ("Verbose is on");
		}
		if (progress > 0)
		{
			msg ("Progress indicator is %d", progress);
		}
		if (quiet)
		{
			msg ("Quiet is on");
		}
		if (retry)
		{
			msg ("Retry enabled");
			msg ("Number of retries is %d", max_retries);
			msg ("Delay is %d seconds", delay_retry);
			msg ("Maximum retry failures allowed is %d", max_retry_failures);
		}
		if (resource != NULL)
		{
			msg ("Resource is %s", resource);
		}
		if (replica != NULL)
		{
			msg ("Replica is %s", replica);
		}
		if (test)
		{
			msg ("Test is on");
		}
		if (utf != NULL)
		{
			msg ("UTF check requested with locale %s", utf);
		}
		if (regexp != NULL)
		{
			msg ("Regexp to match is '%s'", regexp);
		}
		msg ("Directory string is '%s'", directory);
	}

	if (ntasks > 0)
	{
		if (! (PQisthreadsafe()))
		{
			err (FAILURE,
				"Postgres library libpq is not thread safe - confused");
		}
		work = create_work (ntasks, batchsize);
	}

	/* Check directory string. */
	directory_len = strlen (directory);
	if (directory_len == 0)
	{
		err (FAILURE, "Directory string empty - confused");
	}
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

	/* Establish signal handlers. */
	siga (SIGHUP, &sig);
	siga (SIGINT, &sig);
	siga (SIGQUIT, &sig);
	siga (SIGTERM, &sig);

	/* Create global database info block. */
	dbc = create_dbc ();

	/* Mark start. */
	dbc->starttime = time (NULL);
	if (dbc->starttime == (time_t) -1)
	{
		err (FAILURE, "Error getting start time");
	}

	/* Connect to database. */
	conn = PQconnectdb (connect_string);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		perr (CANTCONNECT, conn, "Cannot connect as %s",
			connect_string);
	}
	dbc->conn = conn;

	/* Start transaction block. We are only reading, which is the default. */
	res = pcmd (conn, "BEGIN");
	PQclear (res);

	/* Full pathname. */
	pathname = (char *) allocate (PATHNAME_LENGTH);

	/* Issue Postgres select for the directory tree. */
	hd = select_directories (conn, sort, batchsize, directory);
	dbc->hd = hd;

	/* Go through the directories. */
	fetch (hd);
	dbc->fetches++;
	dbc->rno += (long long unsigned) hd->nrows;
	dbc->dno += (long long unsigned) hd->nrows;
	while (hd->nrows > 0)
	{
		for (i=0; i<(hd->nrows); i++)
		{

			/* Collection internal id and collection name. */
			coll_id = PQgetvalue(hd->res, i, 0);
			dirname = PQgetvalue(hd->res, i, 1);
			(void) strcpy (dbc->last_path, dirname);

			if (dirsonly)
			{

				/* Print directory info when verbose. */
				if (verbose)
				{
					infopath (dirname);
				}
				if (printid)
				{

					/* Print with id. */
					info ("%24s %s", coll_id, dirname);
				}

				/* Execute command  for directory when required. */
				if (command != NULL)
				{
					execute (ntasks, command, dirname);
				}
				if (sqlstmt != NULL)
				{
					execute_sqlstmt (conn, sqlstmt,
						(long long unsigned) atoll (coll_id), dirname);
				}
			}
			else
			{

				/* Now the files in that directory. */
				hf = select_files (conn, sort, batchsize, coll_id);
				dbc->hf = hf;
				fetch (hf);
				dbc->fetches++;
				dbc->rno += (long long unsigned) hf->nrows;
				dbc->fno += (long long unsigned) hf->nrows;
				while ((hf->nrows) > 0)
				{
					for (j=0; j<(hf->nrows); j++)
					{

						/* File name. */
						filename = PQgetvalue(hf->res, j, 2);

						/* File size. */
						filesize = (long long unsigned)
							atol (PQgetvalue(hf->res, j, 1));
						dbc->total += filesize;

						/* File id. */
						fileid = (long long unsigned)
							atol (PQgetvalue(hf->res, j, 0));

						/* Print file info. */
						if ((strlen (dirname) + strlen (filename) + 2) >
							PATHNAME_LENGTH)
						{
							err (FAILURE, "Pathname too long");
						}
						(void) strcpy (pathname, dirname);
						(void) strcat (pathname, "/");
						(void) strcat (pathname, filename);
						(void) strcpy (dbc->last_path, pathname);

						/* Check if matches with regexp if needed. */
						if (verbose)
						{
							infopath (pathname);
						}
						if (printid)
						{

							/* Print with id. */
							info ("%24llu %s", fileid, pathname);
						}
						if (utf != NULL)
						{
							if (! is_utf (pathname))
							{

								/* Print non-conforming path. */
								msg ("%s", pathname);

								/* Execute command for malformed
								   path if there is any. */
								if (command != NULL)
								{
									execute (ntasks, command, pathname);
								}
								dbc->nutfno++;
							}
						}
						else
						{

							/* Generic case. Execute command when required. */
							if (command != NULL)
							{
								execute (ntasks, command, pathname);
							}
							if (sqlstmt != NULL)
							{
								execute_sqlstmt (conn, sqlstmt,
									fileid, pathname);
							}
						}
					}

					/* Next batch of files. */
					fetch (hf);
					dbc->fetches++;
					dbc->rno += (long long unsigned) hf->nrows;
					dbc->fno += (long long unsigned) hf->nrows;
					show_progress (dbc->fetches);
				}
				closecursor (hf);
			}
		}
		fetch (hd);
		dbc->fetches++;
		dbc->rno += (long long unsigned) hd->nrows;
		dbc->dno += (long long unsigned) hd->nrows;
		show_progress (dbc->fetches);
	}
	closecursor (hd);
	free (pathname);

	/* Last flush when needed. */
	if (ntasks > 0)
	{

		/* Flush queue if it was parallel. */
		flush_queue (work);
	}

	/* Finish. */
	res = pcmd (conn, "END");
	PQclear (res);
	PQfinish (conn);
	dbc->endtime = time (NULL);
	if (dbc->endtime == (time_t) -1)
	{
		err (FAILURE, "Error getting end time");
	}
	if (regexp != NULL)
	{
		regfree (rxc);
	}
	if (summary)
	{

		/* Print summary. */
		print_summary (dbc);
	}
	exit (SUCCESS);
} 

/* End of file IFIND.C */

