#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <syscall.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>
#include <sys/time.h>
#include <errno.h>
#include <getopt.h>
#define MAX_SIZE 50
#define NUM_CLIENT 5

void *producer_launcher(void *thread_id);

void sigint(int signo) {
    (void)signo;
}

/**
 *  * Kafka logger callback (optional)
 *   */
static void logger (const rd_kafka_t *rk, int level,
		    const char *fac, const char *buf) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stdout, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
		(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		level, fac, rd_kafka_name(rk), buf);
}

int main()
{

    // Block the SIGINT signal. The threads will inherit the signal mask.
    // This will avoid them catching SIGINT instead of this thread.
    sigset_t sigset, oldset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    pthread_sigmask(SIG_BLOCK, &sigset, &oldset);
    pthread_t client_thread[NUM_CLIENT];
    for (long i=1; i<=NUM_CLIENT; i++) {
        if( pthread_create( &client_thread[i] , NULL ,  producer_launcher , (void*) i) < 0)
        {
            perror("could not create thread");
            return 1;
        }
        sleep(1);
    }

     // Install the signal handler for SIGINT.
     struct sigaction s;
     s.sa_handler = sigint;
     sigemptyset(&s.sa_mask);
     s.sa_flags = 0;
     sigaction(SIGINT, &s, NULL);
    
    // Restore the old signal mask only for this thread.
    /* pthread_sigmask(SIG_SETMASK, &oldset, NULL);
    // Wait for SIGINT to arrive.
     pause();
    for (int i=1;i<=NUM_CLIENT;i++){
     pthread_cancel(client_thread[i]);
    }
     for (int i=1;i<=NUM_CLIENT;i++){
     pthread_join(client_thread[i],NULL);
    }*/

    pthread_exit(NULL);

    return 0;
}


void *producer_launcher(void *threadid)
{
    long threadnum = (long)threadid;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    static rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    unsigned long counter=0;
    char errstr[512];
    char *brokers="10.11.108.11:9092,10.11.108.12:9092,10.11.108.13:9092";
    char *topic="FAULT_EVENT";
    int partition=3;
    long messageCounter=0;
    pthread_t tid = pthread_self();
    printf("POSIX thread id is %d\n", tid);
    int sid = syscall(SYS_gettid);
    char buf[2048];

    // KAFKA CONF              
        conf = rd_kafka_conf_new(); 
        rd_kafka_conf_set_log_cb(conf, logger);
        /* producer config */
       // rd_kafka_conf_set(conf, "queue.buffering.max.messages", "600000", NULL, 0);
       //rd_kafka_conf_set(conf, "message.send.max.retries", "3", NULL, 0);
       // rd_kafka_conf_set(conf, "retry.backoff.ms", "1000", NULL, 0);
        rd_kafka_conf_set(conf, "socket.keepalive.enable", "true", NULL, 0);
       // rd_kafka_conf_set(conf, "request.required.acks", "0", NULL, 0);
       // rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1000", NULL, 0);
        rd_kafka_conf_set(conf, "batch.num.messages", "0", NULL, 0);
                                          
        topic_conf = rd_kafka_topic_conf_new();
                                                 
        /* create Kafka handle */
        if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
         printf("Failed to connect %s",errstr);
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
         exit(1);
        }
        /* Add brokers */
         if (rd_kafka_brokers_add(rk, brokers) == 0) {
            printf("No valid broker found:");
	    fprintf(stderr, "%% No valid brokers specified\n");
	 exit(1);
         }
         /* Create topic */
         rkt = rd_kafka_topic_new(rk, topic, topic_conf);
         topic_conf = NULL; /* Now owned by topic */
                                                                                          
    while(1)
    {
        messageCounter++;
        sprintf(buf, "MESSAGE FROM PRODUCER (%d)", messageCounter);
        size_t len = strlen(buf);
        if (buf[len - 1] == '\n')
        buf[--len] = '\0'; 
        
        /* send/produce message. */
      
        int prod_result=rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                        /* payload and length */
                        buf, len,
                        /* Optional key and its length */
                        NULL, 0,
                        /* Message opaque, provided in
                           * delivery report callback as
                                           * msg_opaque. */
                        NULL);

          printf("Produce return code: %d\n", prod_result);

                if (prod_result == -1) {
                    printf("Error on rd_kafka_produce function call\n");

                    /* Poll to handle delivery reports */
                   // rd_kafka_poll(rk, 0);
                   // run=0;
                   // continue;
                }
        printf("Sent...");
        printf("%% Sent %zd bytes to topic "
				"%s partition %i\n",len, rd_kafka_topic_name(rkt), partition);

        printf("For thread : %d\n", threadnum);
        printf("My Process Id: %d\n",sid);
        printf("Brokers are %s\n",brokers);
        /* Poll to handle delivery reports */
        //rd_kafka_poll(rk, 0);
        sleep(10);
    }

    /* Poll to handle delivery reports */
       //rd_kafka_poll(rk, 0);

        /*int outq=1;

        while (outq > 0) {

            outq = rd_kafka_outq_len(rk);

            printf("%% %i messages in outq\n", outq);

            printf("\n Queue is NOT empty... still waiting!\n\n");

            msSleep(5000);

            rd_kafka_poll(rk, 0);

        }*/
        /* Destroy the handle */
      rd_kafka_destroy(rk);
        printf("Done!");

}


static void msg_delivered (rd_kafka_t *rk,
                           const rd_kafka_message_t *rkmessage, void *opaque) {
    printf("SENT: %s\n",  (const char *)rkmessage->payload);
}

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    printf("ERROR!\n");
}

