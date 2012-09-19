//rb-tree implementation
#include"tree/tree.h"
#include <stddef.h>
#include<stdlib.h>
//random number generator
#include"tinymt/tinymt32.h"
#include<assert.h>
#include<string.h>
//time
#include<sys/time.h>
//leveldb
#include<leveldb/c.h>
#include<stdio.h>

//zeromq
#include<czmq.h>




int
main ()
{
unsigned int N_KEYS;

printf("\n number of keys:");
scanf("%u",&N_KEYS);





  tinymt32_t tinymt32;
//initialize random generator
  tinymt32_init (&tinymt32, 0);


  int iter;
  int stop;
  int counter;


//initialize database
  char *errptr = NULL;

  leveldb_options_t *options = leveldb_options_create ();

/* initializeOptions */
  leveldb_options_set_create_if_missing (options, 1);
  leveldb_options_set_write_buffer_size(options,62914560 );
  leveldb_options_set_max_open_files(options,800000);
/*open Database */

  leveldb_t *db_helper =
    leveldb_open (options, "/mnt/database/database_helper", &errptr);
  leveldb_t *db = leveldb_open (options, "/mnt/database/database", &errptr);



  leveldb_readoptions_t *readoptions = leveldb_readoptions_create ();

  leveldb_writeoptions_t *writeoptions = leveldb_writeoptions_create ();






  int64_t diff = zclock_time ();

//write into database_helper
  for (iter = 0; iter < N_KEYS; iter++)
    {
      unsigned int key = tinymt32_generate_uint32 (&tinymt32);
      unsigned int val = tinymt32_generate_uint32 (&tinymt32);
      leveldb_put (db_helper,
		   writeoptions,
		   (const char *) &key, sizeof (int),
		   (const char *) &val, sizeof (int), &errptr);
      assert (errptr == NULL);
    }

  diff = zclock_time () - diff;

  printf ("\nrandom write:  %d", diff);


  diff = zclock_time ();
//write sequentially into db

  leveldb_iterator_t *liter = leveldb_create_iterator (db_helper,
						       readoptions);

  leveldb_iter_seek_to_first (liter);

  while (leveldb_iter_valid (liter))
    {
      size_t length;
      char key[4];
      memcpy (key, leveldb_iter_key (liter, &length), 4);
      char val[4];
      memcpy (val, leveldb_iter_value (liter, &length), 4);

      leveldb_iter_get_error (liter, &errptr);
      assert (errptr == NULL);
      leveldb_put (db,
		   writeoptions,
		   (const char *) &key, sizeof (int),
		   (const char *) &val, sizeof (int), &errptr);
      assert (errptr == NULL);
      leveldb_iter_next (liter);
    }


  diff = zclock_time () - diff;




  leveldb_close (db);
  leveldb_close (db_helper);





}
