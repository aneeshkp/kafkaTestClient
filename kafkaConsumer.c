#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <syscall.h>
#include <pthread.h>
#define MAX_SIZE 50
#define NUM_CLIENT 5

void *consumer_launcher(void *thread_id);
int main()
{
    pthread_t client_thread[NUM_CLIENT];
    for (long i=1; i<=NUM_CLIENT; i++) {
        if( pthread_create( &client_thread[i] , NULL , consumer _launcher , (void*) i) < 0)
        {
            perror("could not create thread");
            return 1;
        }
        sleep(1);
    }
    pthread_exit(NULL);
    return 0;
}

static void stop (int sig) {
        if (!run)
                exit(1);
	run = 0;
	fclose(stdin); /* abort fgets() */
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

static void hexdump (FILE *fp, const char *name, const void *ptr, size_t len) {
	const char *p = (const char *)ptr;
	unsigned int of = 0;


	if (name)
		fprintf(fp, "%s hexdump (%zd bytes):\n", name, len);

	for (of = 0 ; of < len ; of += 16) {
		char hexen[16*3+1];
		char charen[16+1];
		int hof = 0;

		int cof = 0;
		int i;

		for (i = of ; i < (int)of + 16 && i < (int)len ; i++) {
			hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
			cof += sprintf(charen+cof, "%c",
				       isprint((int)p[i]) ? p[i] : '.');
		}
		fprintf(fp, "%08x: %-48s %-16s\n",
			of, hexen, charen);
	}
}


static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, rk);
}
/**
 *  * Handle and print a consumed message.
 *   * Internally crafted messages are also used to propagate state from
 *    * librdkafka to the application. The application needs to check
 *     * the `rkmessage->err` field for this purpose.
 *      */
static void msg_consume (rd_kafka_message_t *rkmessage,
			 void *opaque) {
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			fprintf(stderr,
				"%% Consumer reached end of %s [%"PRId32"] "
			       "message queue at offset %"PRId64"\n",
			       rd_kafka_topic_name(rkmessage->rkt),
			       rkmessage->partition, rkmessage->offset);

			if (exit_eof && --wait_eof == 0) {
                                fprintf(stderr,
                                        "%% All partition(s) reached EOF: "
                                        "exiting\n");
				run = 0;
                        }

			return;
		}

                if (rkmessage->rkt)
                        fprintf(stderr, "%% Consume error for "
                                "topic \"%s\" [%"PRId32"] "
                                "offset %"PRId64": %s\n",
                                rd_kafka_topic_name(rkmessage->rkt),
                                rkmessage->partition,
                                rkmessage->offset,
                                rd_kafka_message_errstr(rkmessage));
                else
                        fprintf(stderr, "%% Consumer error: %s: %s\n",
                                rd_kafka_err2str(rkmessage->err),
                                rd_kafka_message_errstr(rkmessage));

                if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                    rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
                        run = 0;
		return;
	}

	if (!quiet)
		fprintf(stdout, "%% Message (topic %s [%"PRId32"], "
                        "offset %"PRId64", %zd bytes):\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        rkmessage->partition,
			rkmessage->offset, rkmessage->len);

	if (rkmessage->key_len) {
		if (output == OUTPUT_HEXDUMP)
			hexdump(stdout, "Message Key",
				rkmessage->key, rkmessage->key_len);
		else
			printf("Key: %.*s\n",
			       (int)rkmessage->key_len, (char *)rkmessage->key);
	}

	if (output == OUTPUT_HEXDUMP)
		hexdump(stdout, "Message Payload",
			rkmessage->payload, rkmessage->len);
	else
		printf("%.*s\n",
		       (int)rkmessage->len, (char *)rkmessage->payload);
}

void *consumer_launcher(void *threadid)
{
    long threadnum = (long)threadid;
    char str[15];
    char *group = NULL;
   sprintf(str, "%d", threadnum);
    char *brokers="10.11.108.11:9092,10.11.108.12:9092,10.11.108.13:9092"
    char *topic="FAULT_EVENT"
    int partition=3

    printf("Connected successfully client:%d\n", threadnum);
    pthread_t tid = pthread_self();
    printf("POSIX thread id is %d\n", tid);
    int sid = syscall(SYS_gettid);
    char buf[2048];
    group = "kafka_consumer_";
    strcat(group,str);
   if (rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
       fprintf(stderr, "%% %s\n", errstr);
       exit(1);
      }

    // KAFKA CONF              
        conf = rd_kafka_conf_new(); 
        /* producer config */
       // rd_kafka_conf_set(conf, "queue.buffering.max.messages", "600000", NULL, 0);
       //rd_kafka_conf_set(conf, "message.send.max.retries", "3", NULL, 0);
       // rd_kafka_conf_set(conf, "retry.backoff.ms", "1000", NULL, 0);
        rd_kafka_conf_set(conf, "socket.keepalive.enable", "true", NULL, 0);
       // rd_kafka_conf_set(conf, "request.required.acks", "0", NULL, 0);
       // rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1000", NULL, 0);
        rd_kafka_conf_set(conf, "batch.num.messages", "0", NULL, 0);

       group = "kafka_consumer_";
       strcat(group,str);  
                if (rd_kafka_conf_set(conf, "group.id", group,
                                      errstr, sizeof(errstr)) !=
                    RD_KAFKA_CONF_OK) {
                        fprintf(stderr, "%% %s\n", errstr);
                        exit(1);
                   }
                                          
        topic_conf = rd_kafka_topic_conf_new();
                                                 
        /* create Kafka handle */
        if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)))) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
         exit(1);
        }
        /* Add brokers */
         if (rd_kafka_brokers_add(rk, brokers) == 0) {
	    fprintf(stderr, "%% No valid brokers specified\n");
	 exit(1);
         }
         /* Create topic */
         rkt = rd_kafka_topic_new(rk, topic, topic_conf);
         topic_conf = NULL; /* Now owned by topic */
                                                                                          
        
        /* start consuming */
        if(rd_kafka_consume_start(rkt,partition,start_offset)==-1){
            rd_kafka_resp_err_t err = rd_kafka_last_error();
            fprintf(stderr, "%% Failed to start consuming: %s\n",
	    rd_kafka_err2str(err));
             if (err == RD_KAFKA_RESP_ERR__INVALID_ARG)
                fprintf(stderr, "%% Broker based offset storage "
                                 "requires a group.id, "
                                 "add: -X group.id=yourGroup\n");
		exit(1);
         }

      while(1){
        rd_kafka_messsage_t *rkmessage;
        rd_kafka_resp_err_t err;
        rd _fafka_poll(rk,0);
        rkmessage = rd_kafka_consume(rkt, partition, 1000);
       if (!rkmessage) /* timeout */
        	continue;

         msg_consume(rkmessage, NULL);


        

        printf("For thread : %d\n", threadnum);
        printf("My Process Id: %d\n",sid);
        printf("Brokers are %s\n",brokers)
        /* Poll to handle delivery reports */
        //rd_kafka_poll(rk, 0);
        sleep(2);
    }


     /* Stop consuming */
    rd_kafka_consume_stop(rkt, partition);
     while (rd_kafka_outq_len(rk) > 0)
                 rd_kafka_poll(rk, 10);
		/* Destroy handle */
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

void SendMessage(char buf){
   if (buf[len - 1] == '\n')
        buf[--len] = '\0'; 

      /* Send/Produce message. */

        int prod_result=rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                        /* Payload and length */
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
                    rd_kafka_poll(rk, 0);
                    run=0;
                    continue;
                }

         printf("%% ***** (%lu) Sent %s to topic %s *****\n", counter, buf, topic);

     



}
