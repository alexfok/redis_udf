/*

**    Copyright (c) 2012, Alex Fok alexfokil@gmail.com
**    All rights reserved.
** 
**    This program is free software: you can redistribute it and/or modify
**    it under the terms of the Lesser GNU General Public License as published by
**    the Free Software Foundation, either version 3 of the License, or
**    (at your option) any later version.
**
**    This program is distributed in the hope that it will be useful,
**    but WITHOUT ANY WARRANTY; without even the implied warranty of
**    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
**    Lesser GNU General Public License for more details.
**
**    You should have received a copy of the Lesser GNU General Public License
**    along with this program.  If not, see <http://www.gnu.org/licenses/>.
**
**
**
** Syntax for the new commands are:
** create function <function_name> returns {string|real|integer}
**                soname <name_of_shared_library>
** drop function <function_name>
**
** Each defined function may have a xxxx_init function and a xxxx_deinit
** function.  The init function should alloc memory for the function
** and tell the main function about the max length of the result
** (for string functions), number of decimals (for double functions) and
** if the result may be a null value.
** Compile:
** gcc -shared -o redis_udf.so redis_udf.c  -I "/root/mysql-src/mysql-5.5.21/include" -I "/usr/include/mysql" -fPIC libhiredis.a
** copy the lib to MySQL plugin directory
**	cp redis_udf.so /usr/lib64/mysql/plugin
** After the library is made one must notify mysqld about the new
** functions with the commands:
**      CREATE FUNCTION redis_set RETURNS INTEGER SONAME 'redis_udf.so';
**      CREATE FUNCTION redis_servers_set RETURNS INTEGER SONAME 'redis_udf.so';
**
** to drop the functions, do the following:
**      DROP FUNCTION redis_set;
**      DROP FUNCTION redis_servers_set;
**
** The CREATE FUNCTION and DROP FUNCTION update the func@mysql table.
**
**
** Usage examples:
**     select redis_servers_set('192.168.60.10',6379,'password');
**     select redis_servers_set('192.168.60.10',6379);
**     select redis_set('ro222','1121235');
**     select redis_sadd('ro222','1121235');
**     select redis_srem('ro222','1121235');
**
** Thanks to  Salvatore Sanfilippo <antirez at gmail dot com> and Pieter Noordhuis <pcnoordhuis at gmail dot com> for used hiredis C client
**
**
*/

#include <pthread.h>
#include <strings.h>
#include <sys/time.h>
#include <assert.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

//#include <my_global.h>
#include <my_sys.h>
#include <mysql.h>

#include "fmacros.h"
#include "hiredis.h"

/* Redis context structures and types */
enum connection_type {
    CONN_TCP,
    CONN_UNIX
};

struct config {
    enum connection_type type;

    struct {
        char host[256];
        int port;
    } tcp;

    struct {
        const char *path;
    } unix1;
   char password[256];
   int auth;
   char log_file[256];
   int bdebug;
};

static   struct config cfg = {
        .tcp = {
            .host = "192.168.60.10",
            .port = 6380
        },
        .unix1 = {
            .path = "/tmp/redis.sock"
        },
       .password = "go2oovoo",
       .auth = 0,
       .log_file = "/tmp/redis_udf.log",
       .bdebug = 1,
       .type = CONN_TCP
};

/* Redis Connection pool creation mutex */
pthread_mutex_t sredisContext_mutex = PTHREAD_MUTEX_INITIALIZER;
/* Redis Connection pool object */
static redisContext *sredisContext = NULL;

/* end of Redis context structures and types */


/* These must be right or mysqld will not find the symbol! */
/* Redis Functions prototypes */
my_bool redis_servers_set_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void redis_servers_set_deinit(UDF_INIT *initid);
long long redis_servers_set(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

/* SET Operation */
my_bool redis_set_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void redis_set_deinit(UDF_INIT *initid);
long long redis_set(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

/* SADD */
my_bool redis_sadd_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void redis_sadd_deinit(UDF_INIT *initid);
long long redis_sadd(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

/* SREM */
my_bool redis_srem_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void redis_sadd_deinit(UDF_INIT *initid);
long long redis_srem(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);


/* Helpers */
my_bool _init_redis_command(UDF_INIT *initid, UDF_ARGS *args, char *message);
long long _do_redis_command(char* cmd, UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
redisContext *_myredisConnect(struct config config);
void _myredisDconnect(redisContext *c);
redisContext *_redis_context_init();
void _redis_context_deinit();


/* SADD */
my_bool redis_sadd_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
   return _init_redis_command(initid, args, message);
}

void redis_sadd_deinit(UDF_INIT *initid)
{
}

/***************************************************************************
** UDF long long function.
** Arguments:
** initid       Return value from xxxx_init
** args         The same structure as to xxx_init. This structure
**              contains values for all parameters.
**              Note that the functions MUST check and convert all
**              to the type it wants!  Null values are represented by
**              a NULL pointer
** is_null      If the result is null, one should store 1 here.
** error        If something goes fatally wrong one should store 1 here.
**
** This function should return the result as a long long
***************************************************************************/
long long redis_sadd(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
   return _do_redis_command("SADD", initid, args, is_null, error);
}

/* SREM */
my_bool redis_srem_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
   return _init_redis_command(initid, args, message);
}

void redis_srem_deinit(UDF_INIT *initid)
{
}

long long redis_srem(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
   return _do_redis_command("SREM", initid, args, is_null, error);
}

my_bool redis_set_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
   return _init_redis_command(initid, args, message);
}

void redis_set_deinit(UDF_INIT *initid)
{
}

long long redis_set(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
   return _do_redis_command("SET", initid, args, is_null, error);
}

/*************************************************************************
** Example of init function
** Arguments:
** initid	Points to a structure that the init function should fill.
**		This argument is given to all other functions.
**	my_bool maybe_null	1 if function can return NULL
**				Default value is 1 if any of the arguments
**				is declared maybe_null.
**	unsigned int decimals	Number of decimals.
**				Default value is max decimals in any of the
**				arguments.
**	unsigned int max_length  Length of string result.
**				The default value for integer functions is 21
**				The default value for real functions is 13+
**				default number of decimals.
**				The default value for string functions is
**				the longest string argument.
**	char *ptr;		A pointer that the function can use.
**
** args		Points to a structure which contains:
**	unsigned int arg_count		Number of arguments
**	enum Item_result *arg_type	Types for each argument.
**					Types are STRING_RESULT, REAL_RESULT
**					and INT_RESULT.
**	char **args			Pointer to constant arguments.
**					Contains 0 for not constant argument.
**	unsigned long *lengths;		max string length for each argument
**	char *maybe_null		Information of which arguments
**					may be NULL
**
** message	Error message that should be passed to the user on fail.
**		The message buffer is MYSQL_ERRMSG_SIZE big, but one should
**		try to keep the error message less than 80 bytes long!
**
** This function should return 1 if something goes wrong. In this case
** message should contain something usefull!
**************************************************************************/

/****************************************************************************
** Deinit function. This should free all resources allocated by
** this function.
** Arguments:
** initid	Return value from xxxx_init
****************************************************************************/

/***************************************************************************
** UDF long long function.
** Arguments:
** initid	Return value from xxxx_init
** args		The same structure as to xxx_init. This structure
**		contains values for all parameters.
**		Note that the functions MUST check and convert all
**		to the type it wants!  Null values are represented by
**		a NULL pointer
** is_null	If the result is null, one should store 1 here.
** error	If something goes fatally wrong one should store 1 here.
**
** This function should return the result as a long long
***************************************************************************/
my_bool redis_servers_set_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    redisContext *c = NULL;

   if (args->arg_count < 2 || args->arg_type[0] != STRING_RESULT || args->arg_type[1] != INT_RESULT)
   {
     strncpy(message,"Wrong arguments to Redis function.  Usage: 'key' (string) 'value' (string)", MYSQL_ERRMSG_SIZE);
     return -1;
   }

   if (args->arg_count == 3 && args->arg_type[2] != STRING_RESULT)
   {
     strncpy(message,"Wrong arguments to Redis function1.  Usage: 'key' (string) 'value' (string)", MYSQL_ERRMSG_SIZE);
     return -2;
   }

   strncpy(cfg.tcp.host, (char*)args->args[0], 256);
   cfg.tcp.port = *((longlong*)args->args[1]);
   if (args->arg_count == 3)
   {
      cfg.auth = 1;
      strncpy(cfg.password, (char*)args->args[2], 256);
   }
    _redis_context_deinit();
    c = (redisContext*)_redis_context_init();;
    if (!c)
    {
      strncpy(message, "Failed to connect to Redis", MYSQL_ERRMSG_SIZE);
      return 2;
    }

   return 0;
}

void redis_servers_set_deinit(UDF_INIT *initid)
{
}

long long redis_servers_set(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
   return 0;
//   return _do_redis_command("SET", initid, args, is_null, error);
}


/* Internal Helper Functions */
redisContext *_redis_context_init()
{
    pthread_mutex_lock(&sredisContext_mutex);

    if (!sredisContext)
      sredisContext = _myredisConnect(cfg);
    if (!sredisContext)
    {
       pthread_mutex_unlock(&sredisContext_mutex);
       return NULL;
    }
    pthread_mutex_unlock(&sredisContext_mutex);
    return sredisContext;
}

void _redis_context_deinit()
{
    pthread_mutex_lock(&sredisContext_mutex);

    if (!sredisContext)
      _myredisDconnect(sredisContext);
    sredisContext = NULL;
    pthread_mutex_unlock(&sredisContext_mutex);
    return;
}

my_bool _init_redis_command(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    redisContext *c = NULL;

   if (args->arg_count != 2 || args->arg_type[0] != STRING_RESULT || args->arg_type[1] != STRING_RESULT)
   {
     strncpy(message,"Wrong arguments to Redis function.  Usage: 'key' (string) 'value' (string)", MYSQL_ERRMSG_SIZE);
     return 1;
   }
    c = (redisContext*)_redis_context_init();;
    if (!c)
    {
      strncpy(message, "Failed to connect to Redis", MYSQL_ERRMSG_SIZE);
      return 2;
    }

   return 0;
}

long long _do_redis_command(char* cmd, UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
    redisContext *c = NULL;
    redisReply *reply;
    FILE * pFile;

    *is_null = 0;
    *error = 0;

    c = (redisContext*)_redis_context_init();;
    if (!c)
    {
      *error = 1;
      return -1;
    }
   if (cfg.bdebug)
   {
      pFile = fopen(cfg.log_file,"a");
      if (!pFile)
        return -2;
      fprintf(pFile,"%s %s %s\n",cmd ,args->args[0] ,args->args[1]);
      fclose(pFile);
   }

    reply = redisCommand(c,"%s %s %s",cmd , args->args[0] ,args->args[1]);
//    test_cond(reply->type == REDIS_REPLY_INTEGER && reply->integer == 1)
    freeReplyObject(reply);
    return 0;
}


static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static redisContext *select_database(redisContext *c) {
    redisReply *reply;

    /* Switch to DB 9 for testing, now that we know we can chat. */
    reply = redisCommand(c,"SELECT 9");
    assert(reply != NULL);
    freeReplyObject(reply);

    /* Make sure the DB is emtpy */
    reply = redisCommand(c,"DBSIZE");
    assert(reply != NULL);
    if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 0) {
        /* Awesome, DB 9 is empty and we can continue. */
        freeReplyObject(reply);
    } else {
        printf("Database #9 is not empty: type: %d,  %d, test can not continue\n", reply->type, reply->integer);
        exit(1);
    }

    return c;
}

void _myredisDconnect(redisContext *c)
{
    /* Free the context */
    redisFree(c);
}

redisContext *_myredisConnect(struct config config)
{
    redisContext *c = NULL;
    redisReply *reply;
    char cmd[256];
    int len;

   if (config.type == CONN_TCP) {
        c = redisConnect(config.tcp.host, config.tcp.port);
    } else if (config.type == CONN_UNIX) {
        c = redisConnectUnix(config.unix1.path);
    } else {
        assert(NULL);
    }

    if (c->err) {
        printf("Connection error: %s\n", c->errstr);
	return c;
    }


    /* Authenticate */
    if (config.auth)
    {
       reply = redisCommand(c,"AUTH %s",config.password);
       freeReplyObject(reply);
    }
    return c;
//    return select_database(c);
}
