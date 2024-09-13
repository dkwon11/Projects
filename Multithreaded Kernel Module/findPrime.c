#include <linux/module.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/kthread.h>
#include <linux/slab.h>
#include <linux/atomic.h>
#include <linux/time.h>

DEFINE_SPINLOCK(first_Lock);
DEFINE_SPINLOCK(sec_Lock);

static unsigned long num_threads = 1;
static unsigned long upper_bound = 10;
static struct task_struct **thread_Array;
static int *num_Array;
static int *counter_Array;
static unsigned long long startTime = 0;
static unsigned long long b1Time = 0;
static unsigned long long b2Time = 0;

int pos = 0;
atomic_t check_Fin;
atomic_t b1_threadCounter;
atomic_t b2_threadCounter;

module_param(num_threads,ulong,0);
module_param(upper_bound,ulong,0);


void findNonPrime(int *eachCount){ 
  int posHolder, i;
  while(true){
    spin_lock(&first_Lock);
    posHolder = pos;
    pos ++;

    if(pos == upper_bound-1){
      break;
    } 

    while(pos<upper_bound-1 && num_Array[pos]==0){
      pos++;
    }
    
    spin_unlock(&first_Lock);
    if(posHolder>upper_bound-2){
      break;
    }
    for (i= 2* posHolder +2; i<upper_bound-1; i+= posHolder + 2){
      spin_lock(&sec_Lock);

      (*eachCount)++;
      num_Array[i] = 0;
        
      spin_unlock(&sec_Lock);
    }
  }
  return;
}

static int threadFunc(void *eachCount){
  struct timespec timeHolder;
  
  atomic_add(1,&b1_threadCounter);
  while(atomic_read(&b1_threadCounter) != num_threads){
    //do nothing: waiting for threads to arrive
  }

  getnstimeofday(&timeHolder);
  b1Time = (unsigned long long)timespec_to_ns(&timeHolder); //TIME AFTER FIRST SYNC

  findNonPrime((int*)eachCount);

  atomic_add(1,&b2_threadCounter);
  while(atomic_read(&b2_threadCounter) != num_threads){
    //do nothing: waiting for threads to arrive
  }

  getnstimeofday(&timeHolder);
  b2Time = (unsigned long long)timespec_to_ns(&timeHolder); //TIME AFTER SECOND SYNC

  atomic_set(&check_Fin, 1);

  return 0;
}


static int __init myInit(void){
  struct timespec timeHolder;
  char threadName[256];
  int i;

  getnstimeofday(&timeHolder);
  startTime = (unsigned long long)timespec_to_ns(&timeHolder);

  num_Array = 0;
  counter_Array = 0;
  

  if (upper_bound <= 1 || num_threads <= 0){
    printk("Parameter Error: [Number of Threads (>1) | Upper Bound (>2)]\n");
    upper_bound = 0;
    num_threads = 0;
    return -1;
  }

  atomic_set(&check_Fin, 0); //resetting atomic values (check_fin will be 1 if complete)
  atomic_set(&b1_threadCounter, 0);
  atomic_set(&b2_threadCounter, 0);
  
  num_Array = kmalloc(sizeof(int) * (upper_bound - 1), GFP_KERNEL);

  if (num_Array == NULL){
    printk("Memory Allocation Error on Number Array!\n");
    upper_bound = 0;
    num_threads = 0;
    num_Array = 0;
    return -1;
  }

  counter_Array = kmalloc(sizeof(int) * num_threads, GFP_KERNEL);

  if (counter_Array == NULL){
    printk("Memory Allocation Error on Counter Array!\n");
    upper_bound = 0;
    num_threads = 0;
    num_Array = 0;
    counter_Array = 0;
    kfree(num_Array);
    return -1;
  }

  for (i=0;i<upper_bound-1;i++){ //filling up num array
    num_Array[i] = i + 2;
  }

  for (i=0;i<num_threads;i++){ //filling up counter array
    counter_Array[i] = 0;
  }

  thread_Array = kmalloc (sizeof(struct task_struct*) * num_threads, GFP_KERNEL);

  for (i=0;i<num_threads;i++){ //creating threads and giving them number labels for the name param.
    sprintf(threadName,"%d", i);
    thread_Array[i] = kthread_create(threadFunc, (void *)&counter_Array[i], threadName);
    wake_up_process(thread_Array[i]);
  }

  return 0;

}

static void __exit myExit(void){
  int num_Prime, num_XPrime, X_Counter, i;
  num_Prime = 0;
  num_XPrime = 0;
  X_Counter = 0;

  if(atomic_read(&check_Fin) == 0){ //checking if all processes are completed
    printk("PROCESSING NOT COMPLETED! EXITING MODULE. \n");
    return;
  }
  printk("\n <DATA REPORT FOR %lu THREADS and UPPERBOUND of %lu> \n", num_threads, upper_bound);
  printk("\n <---------Prime Numbers---------> \n");
  for (i=0;i<upper_bound-1;i++){ //print primes
    if (num_Array[i] != 0){
      num_Prime ++;
      printk("%d ", num_Array[i]);
      if(num_Prime %8 == 0){ //formatting (8 per line)
        printk("\n");
      }
    }
  }

  num_XPrime = upper_bound - 1 - num_Prime; //true count of numbers that aren't prime 

  for (i = 0; i < num_threads; i++){ //counting the number of crosses amongst all threads (expected to be larger than true count)
    X_Counter += counter_Array[i];
  }

  printk("\n <---------Analytics---------> \n");
  printk("Prime Number count: %d\n", num_Prime);
  printk("Non-Prime Number count: %d\n", num_XPrime);
  printk("Cross count: %d\n", X_Counter);
  printk("Extra Crosses: %d\n\n", X_Counter - num_XPrime);
  printk("Init Time: %llu\n", b1Time - startTime);
  printk("Computation Time: %llu\n", b2Time - b1Time);
  printk("Completion Time: %llu\n", (b1Time - startTime) + (b2Time - b1Time));

  kfree(num_Array);
  kfree(counter_Array);

  printk("Module Unloaded! All memories freed.\n");
  return;
}

module_init(myInit);
module_exit(myExit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("DK");
MODULE_DESCRIPTION("Find Primes");
