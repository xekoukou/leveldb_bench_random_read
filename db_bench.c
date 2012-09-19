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



typedef struct
{
  leveldb_t **db;
  leveldb_options_t **options;
  leveldb_readoptions_t *readoptions;
  leveldb_writeoptions_t *writeoptions;
} worker_args_t;




struct dbkey_t
{
  unsigned int key;
    RB_ENTRY (dbkey_t) field;
};


int
cmp_dbkey_t (struct dbkey_t *first, struct dbkey_t *second)
{

  if (first->key > second->key)
    {
      return 1;
    }
  else
    {
      if (first->key < second->key)
	{
	  return -1;
	}
      else
	{
	  return 0;
	}
    }

}

RB_HEAD (dbkey_rb_t, dbkey_t);
RB_GENERATE (dbkey_rb_t, dbkey_t, field, cmp_dbkey_t);

typedef struct dbkey_t dbkey_t;





void
worker_fn (void *arg, zctx_t * ctx, void *pipe)
{
  int rc;
  worker_args_t *args = (worker_args_t *) arg;

//connect 
  void *pull = zsocket_new (ctx, ZMQ_PULL);
  rc = zsocket_connect (pull, "inproc://bind_point");

//connect 
  void *sub = zsocket_new (ctx, ZMQ_SUB);
  rc = zsocket_connect (sub, "inproc://pub_point");

//connect 
  void *dealer = zsocket_new (ctx, ZMQ_DEALER);
  rc = zsocket_connect (dealer, "inproc://response_point");

//subscribing to all messages
  zsocket_set_subscribe (sub, "");


  unsigned int counter = 0;
  size_t vallen;
  char *errptr;
  int stop = 0;
  int timeout = -1;


  zmq_pollitem_t pollitem[] = {
    {pull, 0, ZMQ_POLLIN, 0},
    {sub, 0, ZMQ_POLLIN, 0},
  };

  while (1)
    {
      rc = zmq_poll (pollitem, 2, timeout);
      assert (rc != -1);

      if (pollitem[0].revents & ZMQ_POLLIN)
	{
	  unsigned int key;
	  zframe_t *frame = zframe_recv_nowait (pull);
	  memcpy (&key, zframe_data (frame), 4);
	  zframe_destroy (&frame);

	  free (leveldb_get (*(args->db),
			     args->readoptions,
			     (const char *) &key, 4, &vallen, &errptr));
	  counter++;
	}
      else if (stop)
	{ 
          zframe_t *frame=zframe_new (&counter, 4);
	  zframe_send (&frame, dealer, 0);
	  counter = 0;
	  stop = 0;
	  timeout = -1;
	}

      if (pollitem[1].revents & ZMQ_POLLIN)
	{
	  zframe_t *frame = zframe_recv_nowait (sub);
	  memcpy (&stop, zframe_data (frame), 4);
	  zframe_destroy (&frame);
	  if (stop)
	    {
	      timeout = 0;
	    }

	}



    }
}

void benchmark_notree(void * push,void *pub,void *router,unsigned int N_KEYS){					
printf ("\nCleaning the pagecache");                   		
								
/*cleaning cache */						
  system ("./script.sh");					
								
  printf ("\n starting random read without a rb_btree");        
                       
tinymt32_t tinymt32;                                         
/*initializing random generator with the same seed */           
  tinymt32_init (&tinymt32, 0);                                 
                                                                
                                                                
  int64_t diff = zclock_time ();                                        
  unsigned int iter;  
  int stop;
  unsigned int counter=0;
                                                            
  for (iter = 0; iter < N_KEYS; iter++)                         
    {                                                           
      unsigned int key = tinymt32_generate_uint32 (&tinymt32);  
      tinymt32_generate_uint32 (&tinymt32);                     
      size_t vallen;                                            
                                                                
      zframe_t *frame = zframe_new (&key, 4);                   
      zframe_send (&frame, push, 0);                            
                                                                
    }                                                           
  stop = 1;                                                     
  zframe_t * frame=zframe_new (&stop, 4);                       
  zframe_send (&frame, pub, 0);                                 
                                                                
  for (iter = 0; iter < 6; iter++)                              
    {                                                           
      unsigned int temp;                                        
      zmsg_t *msg = zmsg_recv (router);                        
      zframe_t *frame=zmsg_unwrap (msg);                       
      zframe_destroy (&frame);                                  
      memcpy (&temp, zframe_data (zmsg_first (msg)), 4);        
      counter = counter + temp;                                 
    }                                                           
                                                                
  printf("\nkeys processed:%u", counter);                                        
                                                                
  diff = zclock_time () - diff;                                
                                                                
  float stat = ((float) counter * 1000) / (float) diff;          
  printf ("\nrandom read without an rb_tree:  %f keys per sec\n", stat); 
}


void benchmark_tree(void * push,void *pub,void *router,unsigned int N_KEYS,struct dbkey_rb_t  dbkey_rb, unsigned int latency){
                                               
/*cleaning cache   */                                                
  system ("./script.sh");                                            
                   

tinymt32_t tinymt32;                                                  
/*initializing random generator with the same seed  */               
  tinymt32_init (&tinymt32, 0);                                     
                                                                     
                                                                     
  float stat;                                                                   
  int64_t diff = zclock_time ();                                     
  unsigned int iter;                                                              
  int stop;                                                           
  unsigned int counter=0;                                              
                                                                     
                                                                     
  iter = 0;                                                          
  while (iter < N_KEYS)                                              
    {                                                                
      dbkey_t *dbkey;                                                
      size_t vallen;                                                 
                                                                     
      int64_t diff2 = zclock_time ();                                
      while (zclock_time () - diff2 < latency)                             
        {                                                            
                                                                     
          dbkey = (dbkey_t *) malloc (sizeof (dbkey_t));             
          dbkey->key = tinymt32_generate_uint32 (&tinymt32);         
          tinymt32_generate_uint32 (&tinymt32);                      
          RB_INSERT (dbkey_rb_t, &dbkey_rb, dbkey);                  
          if (iter == N_KEYS - 1)                                    
            {                                                        
              iter++;                                                
              break;                                                 
            }                                                        
          else {                                                       
                                                                     
              iter++;                                                
            }                                                        
        }                                                            
                                                                     
      dbkey_t *tr_iter = RB_MIN (dbkey_rb_t, &dbkey_rb);             
                                                                     
      while (tr_iter)                                                
        {                                                            
                                                                     
          zframe_t *frame = zframe_new (&(tr_iter->key), 4);         
          zframe_send (&frame, push, 0);                             
                                                                     
                                                                     
          dbkey_t *temp = tr_iter;                                   
          tr_iter = RB_NEXT (dbkey_rb_t, &dbkey_rb, tr_iter);        
                                                                     
          RB_REMOVE (dbkey_rb_t, &dbkey_rb, temp);                   
          free (temp);                                               
        }                                                            
                                                                     
                                                                     
    }                                                                
                                                                     
  stop = 1;                                                          
  zframe_t *frame=zframe_new (&stop, 4);                                       
  zframe_send (&frame, pub, 0);                                      
                                                                     
  for (iter = 0; iter < 6; iter++)                                   
    {                                                                
      unsigned int temp;                                             
      zmsg_t *msg = zmsg_recv (router);                              
      zframe_t *frame=zmsg_unwrap (msg);                             
      zframe_destroy (&frame);                                       
      memcpy (&temp, zframe_data (zmsg_first (msg)), 4);             
      counter = counter + temp;                                      
    }                                                                
                                                                     
  printf("\nkeys processed:%u", counter);                                        
                                                                     
  diff = zclock_time () - diff;                                      
                                                                     
  stat = ((float) counter * 1000) / (float) diff;                     
  printf ("\nrandom read with an rb_tree:  %f keys per sec\n", stat);

}


int
main ()
{

int64_t diff;
unsigned int N_KEYS;
unsigned int capacity;
unsigned int latency;

printf("\n number of keys:");
scanf("%u",&N_KEYS);

printf("\n Cache size(bytes):");
scanf("%u",&capacity);

printf("\n latency:");
scanf("%u",&latency);
//zeromq context
  zctx_t *ctx = zctx_new ();

//initialize worker args
  worker_args_t worker_args;


//initialize rb_tree;
  struct dbkey_rb_t dbkey_rb;
  RB_INIT (&dbkey_rb);





//initialize database
  char *errptr = NULL;

  leveldb_options_t *options = leveldb_options_create ();
  worker_args.options = &options;

/* initialize Options */
  leveldb_options_set_create_if_missing (options, 1);
  leveldb_options_set_write_buffer_size(options,62914560 );
  leveldb_options_set_max_open_files(options,800000);
//bloom filter
leveldb_filterpolicy_t* bloom=leveldb_filterpolicy_create_bloom(10);
  leveldb_options_set_filter_policy(
    options,
    bloom);

//cache
leveldb_cache_t* cache;



  leveldb_readoptions_t *readoptions = leveldb_readoptions_create ();
  worker_args.readoptions = readoptions;

//cache
leveldb_readoptions_set_fill_cache(
    readoptions, 1);



  leveldb_writeoptions_t *writeoptions = leveldb_writeoptions_create ();
  worker_args.writeoptions = writeoptions;


  int rc;
  void *push = zsocket_new (ctx, ZMQ_PUSH);
  
  int water=2000000000;
zsocket_set_hwm (push,water);

  rc = zsocket_bind (push, "inproc://bind_point");

  //connect 
  void *pub = zsocket_new (ctx, ZMQ_PUB);
  rc = zsocket_bind (pub, "inproc://pub_point");

//connect 
  void *router = zsocket_new (ctx, ZMQ_ROUTER);
  rc = zsocket_bind (router, "inproc://response_point");

//sleep a while
  zclock_sleep (1000);

//this assumes some synchronization at the start and end of each new bench
  leveldb_t *db_pointer;
  worker_args.db = &db_pointer;

//initialize the threads
  void *pipe[6];

  unsigned char i;
  for (i = 0; i < 6; i++)
    {
      pipe[i] = zthread_fork (ctx, &worker_fn, (void *) &(worker_args));
    }

//sleep a while
  zclock_sleep (1000);




  db_pointer = leveldb_open (options, "/mnt/database/database", &errptr);


cache= leveldb_cache_create_lru(capacity);
 benchmark_notree(push,pub,router,N_KEYS);




  printf
    ("\n starting random read without a rb_btree (second pass that looks at warm cache benefits)");


 benchmark_notree(push,pub,router,N_KEYS);

leveldb_cache_destroy(cache);

printf("\n benchmark with tree 1st pass");
  
  leveldb_close (db_pointer);

  db_pointer = leveldb_open (options, "/mnt/database/database", &errptr);

cache= leveldb_cache_create_lru(capacity);
  benchmark_tree(push,pub,router,N_KEYS,dbkey_rb,latency);

printf("\nsecond pass");


  benchmark_tree(push,pub,router,N_KEYS,dbkey_rb,latency);

leveldb_cache_destroy(cache);
  leveldb_close (db_pointer);

  db_pointer =
    leveldb_open (options, "/mnt/database/database_helper", &errptr);



  printf
    ("\n starting random read without a rb_btree on random inserted database");


cache= leveldb_cache_create_lru(capacity);
 benchmark_notree(push,pub,router,N_KEYS);
  printf("\nsecond pass");
 benchmark_notree(push,pub,router,N_KEYS);

leveldb_cache_destroy(cache);
  leveldb_close (db_pointer);

cache= leveldb_cache_create_lru(capacity);
  db_pointer =
    leveldb_open (options, "/mnt/database/database_helper", &errptr);

 printf("\n with a tree");

  benchmark_tree(push,pub,router,N_KEYS,dbkey_rb,latency);

printf("\n second pass");

  benchmark_tree(push,pub,router,N_KEYS,dbkey_rb,latency);

leveldb_cache_destroy(cache);
leveldb_close (db_pointer);
}
