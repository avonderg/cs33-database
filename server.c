#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "./comm.h"
#include "./db.h"

/*
 * Use the variables in this struct to synchronize your main thread with client
 * threads. Note that all client threads must have terminated before you clean
 * up the database.
 */
typedef struct server_control {
    pthread_mutex_t server_mutex;
    pthread_cond_t server_cond;
    int num_client_threads;
} server_control_t;

/*
 * Controls when the clients in the client thread list should be stopped and
 * let go.
 */
typedef struct client_control {
    pthread_mutex_t go_mutex;
    pthread_cond_t go;
    int stopped;
} client_control_t;

/*
 * The encapsulation of a client thread, i.e., the thread that handles
 * commands from clients.
 */
typedef struct client {
    pthread_t thread;
    FILE *cxstr;  // File stream for input and output

    // For client list
    struct client *prev;
    struct client *next;
} client_t;

/*
 * The encapsulation of a thread that handles signals sent to the server.
 * When SIGINT is sent to the server all client threads should be destroyed.
 */
typedef struct sig_handler {
    sigset_t set;
    pthread_t thread;
} sig_handler_t;

client_t *thread_list_head;
pthread_mutex_t thread_list_mutex = PTHREAD_MUTEX_INITIALIZER;

void *run_client(void *arg);
void *monitor_signal(void *arg);
void thread_cleanup(void *arg);

// 1 if being accepted, 0 otherwise
int accepted = 1;
server_control_t server = {PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0};

// Called by client threads to wait until progress is permitted
void client_control_wait() {
    // TODO: Block the calling thread until the main thread calls
    // client_control_release(). See the client_control_t struct.

}

// Called by main thread to stop client threads
void client_control_stop() {
    // TODO: Ensure that the next time client threads call client_control_wait()
    // at the top of the event loop in run_client, they will block.
}

// Called by main thread to resume client threads
void client_control_release() {
    // TODO: Allow clients that are blocked within client_control_wait()
    // to continue. See the client_control_t struct.
}

// Called by listener (in comm.c) to create a new client thread
void client_constructor(FILE *cxstr) { // is server()
    // You should create a new client_t struct here and initialize ALL
    // of its fields. Remember that these initializations should be
    // error-checked.
    //
    // TODO:
    // Step 1: Allocate memory for a new client and set its connection stream
    // to the input argument.
    // Step 2: Create the new client thread running the run_client routine.
    // Step 3: Detach the new client thread

    // create and start a thread
    client_t *newClient;
    if ((newClient = malloc(sizeof(client_t))) == 0) {
        perror("malloc");
        exit(1);
    }
    // what do i do if cxstr is null
    newClient->cxstr = cxstr;
    newClient->next = NULL;
    newClient->prev = NULL;
    int err1;
    if ((err1 = pthread_create(&newClient->thread,0,run_client,newClient)) != 0) {
        handle_error_en(err1, "pthread_create");
    }
    int err2;
    if ((err2 = pthread_detach(newClient->thread)) != 0) {
        handle_error_en(err2, "pthread_detach");
    }
}

void client_destructor(client_t *client) {
    // TODO: Free and close all resources associated with a client.
    // Whatever was malloc'd in client_constructor should
    // be freed here!
    comm_shutdown(client->cxstr);
    free(client);
}

// Code executed by a client thread
void *run_client(void *arg) {
    // TODO:
    // Step 1: Make sure that the server is still accepting clients. This will
    //         will make sense when handling EOF for the server.
    // Step 2: Add client to the client list and push thread_cleanup to remove
    //       it if the thread is canceled.
    // Step 3: Loop comm_serve (in comm.c) to receive commands and output
    //       responses. Execute commands using interpret_command (in db.c)
    // Step 4: When the client is done sending commands, exit the thread
    //       cleanly.
    //
    // You will need to modify this when implementing functionality for stop and
    // go!
    client_t *client = (client_t *) arg;
    int err1;
    if ((err1 = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(err1, "pthread_mutex_lock");
    }
    // ensure that clients are still being accepted
    if (accepted == 1) { 
        // use prev and next to insert
        if (thread_list_head == NULL) {
            thread_list_head = client;
            client->prev = NULL;
            client->next = NULL;
        }
        else {
            client_t *curr = thread_list_head;
            while (curr->next != NULL) {
                curr = curr->next;
            }
            curr->next = client;
            client->prev = curr;
            client->next = NULL;
        }
        int err2;
        if ((err2 = pthread_mutex_lock(&server.server_mutex)) != 0) { // lock to increment # clients
            handle_error_en(err2, "pthread_mutex_lock");
        }
        server.num_client_threads++;
        int err3;
        if ((err3 = pthread_mutex_unlock(&server.server_mutex)) != 0) { // lock to increment # clients
            handle_error_en(err3, "pthread_mutex_unlock");
        }
        int err4;
        if ((err4 = pthread_mutex_unlock(&thread_list_mutex)) != 0) { // lock to increment # clients
            handle_error_en(err4, "pthread_mutex_unlock");
        }
        pthread_cleanup_push(thread_cleanup,client);
        // here, we process the commands
        // createa a buffer and call memset
        char response[BUFLEN];
        char command[BUFLEN];
        memset(command,0,BUFLEN);
        memset(response,0,BUFLEN);
            while (comm_serve(client->cxstr,response,command) == 0) { //cancellation point
                client_control_wait(); // make sure running client (if stop = true), makes sure threads wait
                interpret_command(command,response,BUFLEN);
            }
        pthread_cleanup_pop(1);
        return NULL;
    }
    else {
        client_destructor(client);
        int err;
        if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) { 
            handle_error_en(err, "pthread_mutex_unlock");
        }
        return NULL;
    }
}
// before you call, make sure thread list mutex is locked
void delete_all() { // make sure you don't cancel before you pushed to cleanup (mutexes)
    // TODO: Cancel every thread in the client thread list with the
    // pthread_cancel function.
    // check handout
    client_t *curr = thread_list_head;
    while (curr->next != NULL) {
        int err;
        if ((err = pthread_cancel(curr->thread)) != 0) { 
            handle_error_en(err, "pthread_cancel");
        }
        pthread_mutex_lock(&server.num_client_threads);
        server.num_client_threads = 0;
        pthread_mutex_unlock(&server.num_client_threads);
        curr = curr->next;
    }
    return NULL;
}

// Cleanup routine for client threads, called on cancels and exit.
void thread_cleanup(void *arg) {
    // TODO: Remove the client object from thread list and call
    // client_destructor. This function must be thread safe! The client must
    // be in the list before this routine is ever run.
    client_t *client = (client_t *) arg;
    pthread_mutex_lock(&thread_list_mutex);
    client_t *curr = thread_list_head;
    if (client == thread_list_head) { // if it is at the head of list
        thread_list_head = client->next;
    }
    else if (client->next == NULL) { // if it is at the end of the list
        client->prev->next = NULL;
    }
    else {
        while (curr->next != client) {
        curr = curr->next;
        }
        curr->next = client->next;
        // curr->prev = curr;
        // client->prev = curr;
        client->next->prev = client->prev;
    }
    pthread_mutex_unlock(&thread_list_mutex);
    client_destructor(client);
    return NULL;
}

// Code executed by the signal handler thread. For the purpose of this
// assignment, there are two reasonable ways to implement this.
// The one you choose will depend on logic in sig_handler_constructor.
// 'man 7 signal' and 'man sigwait' are both helpful for making this
// decision. One way or another, all of the server's client threads
// should terminate on SIGINT. The server (this includes the listener
// thread) should not, however, terminate on SIGINT!
void *monitor_signal(void *arg) {
    // TODO: Wait for a SIGINT to be sent to the server process and cancel
    // all client threads when one arrives.
    sigset_t *sig = (sigset_t *) arg;
    int signal;
    while(1) {
      sigwait(sig,&signal);
      if (signal == SIGINT){
        pthread_mutex_lock(&thread_list_mutex);
        delete_all();
        pthread_mutex_unlock(&thread_list_mutex);
      } 
    }
    return NULL;
}

sig_handler_t *sig_handler_constructor() {
    // TODO: Create a thread to handle SIGINT. The thread that this function
    // creates should be the ONLY thread that ever responds to SIGINT.
    sig_handler_t *sig;
    if ((sig = malloc(sizeof(sig_handler_t))) == 0) {
        perror("malloc");
        exit(1);
    }
    sigemptyset(&sig->set);
    sigaddset(&sig->set, SIGINT);
    // create thread
    pthread_sigmask(SIG_BLOCK, &sig->set, 0); // error check
    pthread_create(&sig->thread, 0, monitor_signal, &sig->set); // error check
    return sig;
}

void sig_handler_destructor(sig_handler_t *sighandler) {
    // TODO: Free any resources allocated in sig_handler_constructor.
    // Cancel and join with the signal handler's thread.
    pthread_cancel(sighandler->thread); // error check
    pthread_join(sighandler->thread, NULL); // error check
    free(sighandler);
}

// The arguments to the server should be the port number.
// returns 1 if there was an error
int main(int argc, char *argv[]) {
    // TODO:
    // Step 1: Set up the signal handler for handling SIGINT.
    // Step 2: ignore SIGPIPE so that the server does not abort when a client
    // disocnnects Step 3: Start a listener thread for clients (see
    // start_listener in
    //       comm.c).
    // Step 4: Loop for command line input and handle accordingly until EOF.
    // Step 5: Destroy the signal handler, delete all clients, cleanup the
    //       database, cancel and join with the listener thread
    //
    // You should ensure that the thread list is empty before cleaning up the
    // database and canceling the listener thread. Think carefully about what
    // happens in a call to delete_all() and ensure that there is no way for a
    // thread to add itself to the thread list after the server's final
    // delete_all().
    sigset_t signal;
    sigemptyset(&signal);
    sigaddset(&signal, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &signal, NULL); // error check
    sig_handler_t *sig = sig_handler_constructor();
    pthread_t listener = start_listener(atoi(argv[1]), client_constructor);
    // how do i access source and dest buffers created within run_client
    // accepted = 1;
    while (1) { // step 4
        char buf[BUFLEN];
        memset(buf, '\0', BUFLEN);
        int fd = STDIN_FILENO;
        size_t count = BUFLEN;
        ssize_t to_read;
        // to_read = read(source, buf, count);
        // write(dest,buf,to_read)
        to_read = read(fd, buf, count); // read from input
        if (to_read == -1) {
            perror("error: read");
            return 1;
        }
        // else if (to_read == 0) {  // restart program
        //     accepted = 0;
        //     pthread_cancel();
        //     pthread_exit();
        // }
        if (buf[0] == 's') {
            client_control_stop();
        }
        else if (buf[0] == 'g') {
            client_control_release();
        }
        else if (buf[0] == 'p') {
            if (buf[1] != NULL) {
                db_print(&buf[1]);
            }
            else {
                db_print(stdout);
            }
        }
        // read stop go, etc.. call appropriate commands
        // buf at index zero (as long as to_read >0)
    }
    // set accepted to 0 when have EOF (stop accepting)
    sig_handler_destructor(sig);
    // cleanup follows...
    return 0;
}
