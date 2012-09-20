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
unsigned int N_BATCH;

printf("\n number of keys:");
scanf("%u",&N_KEYS);

printf("\n batch size:");
scanf("%u",&N_BATCH);






  tinymt32_t tinymt32;
//initialize random generator
  tinymt32_init (&tinymt32, 0);


  int iter;
  int sec_iter;
  int stop;
  int counter;


//initialize database
  char *errptr = NULL;

  leveldb_options_t *options = leveldb_options_create ();

/* initializeOptions */
  leveldb_options_set_create_if_missing (options, 1);
  leveldb_options_set_write_buffer_size(options,120000000 );
  leveldb_options_set_max_open_files(options,800000);
/*open Database */

  leveldb_t *db_helper =
    leveldb_open (options, "/mnt/database/database_helper", &errptr);
  leveldb_t *db = leveldb_open (options, "/mnt/database/database", &errptr);



  leveldb_readoptions_t *readoptions = leveldb_readoptions_create ();

  leveldb_writeoptions_t *writeoptions = leveldb_writeoptions_create ();

leveldb_writebatch_t* batch=leveldb_writebatch_create();


  int64_t diff = zclock_time ();

//write into database_helper
  iter=0;
  while( iter < N_KEYS)
    {
     sec_iter=0;
     while((iter<N_KEYS) && (sec_iter<N_BATCH)){
      unsigned int key = tinymt32_generate_uint32 (&tinymt32);
      unsigned int val = tinymt32_generate_uint32 (&tinymt32);
      leveldb_writebatch_put (batch,
		   (const char *) &key, sizeof (int),
		   (const char *) &val, sizeof (int));
    sec_iter++;
    iter++;
    }
      leveldb_write(
    db_helper,
    writeoptions,
    batch,
    &errptr);

      if(errptr!=NULL){
      printf("\n%s",errptr);
      }
      assert (errptr == NULL);
      leveldb_writebatch_clear(batch);
    }
  
      leveldb_writebatch_destroy(batch);

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
