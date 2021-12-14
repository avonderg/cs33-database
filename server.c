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


/*
 * Global variable indicating if the server is accepting new client connections.
 * Set to 1 if new connections being accepted, and zero otherwise. 
 */
int accepted = 1;

/*
 * Initialization of instances of server control and client control structs.
 */
server_control_t server = {PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0};
client_control_t client = {PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0};


/**
 * Called by client threads to wait until progress is permitted. Blocks the calling thread
 * until the main thread calls client_control_release.
 * **/
void client_control_wait() {
   pthread_mutex_lock(&client.go_mutex);
   pthread_cleanup_push(pthread_mutex_unlock, &client.go_mutex); // cancellation point (CAST TO VOID???)
    while (client.stopped) { // if stopped = 1, it has to wait
        pthread_cond_wait(&client.go, &client.go_mutex);
    }
   pthread_cleanup_pop(1);
}


/**
 * Called by main thread to stop client threads. Ensures that the next time
 * client threads call client_control_wait() at the top of the event loop in 
 * run_client, they will block.
 * **/
void client_control_stop() {
    pthread_mutex_lock(&client.go_mutex);
    client.stopped = 1; // sets stopped to be true
    fprintf(stderr, "stopping all clients\n");
    pthread_mutex_unlock(&client.go_mutex);
}


/**
 * Called by main thread to resume client threads. Allows clients that are 
 * blocked within client_control_wait() to continue.
 * **/
void client_control_release() {
    pthread_mutex_lock(&client.go_mutex);
    client.stopped = 0; // sets stopped to be false
    pthread_cond_broadcast(&client.go); // broadcasts to waiting clients
    fprintf(stderr, "releasing all clients\n");
    pthread_mutex_unlock(&client.go_mutex);
}

/**
 * Called by listener (in comm.c) to create a new client thread. Creates a new client_t
 * struct and initializes all of its fields.
 *
 * Parameters:
 * - cxstr: a pointer to the file stream for the client
 * **/
void client_constructor(FILE *cxstr) {
    // You should create a new client_t struct here and initialize ALL
    // of its fields. Remember that these initializations should be
    // error-checked.
    //
    // TODO:
    // Step 1: Allocate memory for a new client and set its connection stream
    // to the input argument.
    // Step 2: Create the new client thread running the run_client routine.
    // Step 3: Detach the new client thread

    // allocates memory for the new client
    client_t *newClient;
    if ((newClient = malloc(sizeof(client_t))) == 0) {
        perror("malloc");
        exit(1);
    }
    // initializes fields of the new client
    // what do i do if cxstr is null
    newClient->cxstr = cxstr;
    newClient->next = NULL;
    newClient->prev = NULL;
    int err1;
    if ((err1 = pthread_create(&newClient->thread,0,run_client,newClient)) != 0) { // creates a thread
        handle_error_en(err1, "pthread_create");
    }
    int err2;
    if ((err2 = pthread_detach(newClient->thread)) != 0) { // detaches the new client thread
        handle_error_en(err2, "pthread_detach");
    }
}

/**
 * Frees and closes all resources associated with a client.
 *
 * Parameters:
 * - client: a pointer to the client_t client to be destroyed
 * **/
void client_destructor(client_t *client) {
    // TODO: Free and close all resources associated with a client.
    // Whatever was malloc'd in client_constructor should
    // be freed here!
    comm_shutdown(client->cxstr);
    free(client);
}

/**
 * Code executed by a client thread. Adds a client to the client list, and pushes 
 * thread_cleanup to remove it if the thread is cancelled. Loops comm.serve to receive
 * commands and output responses, and executes commands. 
 *
 * Parameters:
 * - arg: a void pointer to the client_t client to be run
 * 
 * Returns:
 * - NULL if successful
 * **/
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
    pthread_mutex_lock(&thread_list_mutex);
    if (accepted == 1) {  // if clients are still being accepted
        // uses prev and next fields to insert
        if (thread_list_head == NULL) { // if the head of the list is null, insert
            thread_list_head = client;
            client->prev = NULL;
            client->next = NULL;
        }
        else { // otherwise, add it to the end of the list
            client_t *curr = thread_list_head;
            while (curr->next != NULL) {
                curr = curr->next;
            }
            curr->next = client;
            client->prev = curr;
            client->next = NULL;
        }
        pthread_mutex_unlock(&thread_list_mutex);
        pthread_mutex_lock(&server.server_mutex);// lock to increment # clients
        server.num_client_threads++;
        pthread_mutex_unlock(&server.server_mutex);
        pthread_cleanup_push(thread_cleanup,client); // push thread_cleanup
        // here, we process the commands, create a buffer and call memset
        char response[BUFLEN];
        char command[BUFLEN];
        memset(command,0,BUFLEN);
        memset(response,0,BUFLEN);
        while (comm_serve(client->cxstr,response,command) == 0) { // cancellation point
            client_control_wait(); // makes sure running client (if stop = true), ie, makes sure threads wait
            interpret_command(command,response,BUFLEN); // interprets command
        }
        pthread_cleanup_pop(1);
        return NULL;
    }
    else {
        client_destructor(client);
        pthread_mutex_unlock(&thread_list_mutex);
        return NULL;
    }
}

/**
 * Cancels every thread in the client thread list with the pthread_cancel function.
 * **/
void delete_all() {
    // TODO: Cancel every thread in the client thread list with the
    // pthread_cancel function.
    client_t *curr = thread_list_head;
    while (curr != NULL) { // loops through thread list to cancel each client
        int err;
        if ((err = pthread_cancel(curr->thread)) != 0) { 
            handle_error_en(err, "pthread_cancel");
        }
        curr = curr->next;
    }
    return;
}

/**
 * Cleanup routine for client threads, called on cancels and exit. Removes the client
 * object from the thread list and calls client_destructor.
 *
 * Parameters:
 * - arg: a void pointer to the client_t client
 * **/
void thread_cleanup(void *arg) {
    // TODO: Remove the client object from thread list and call
    // client_destructor. This function must be thread safe! The client must
    // be in the list before this routine is ever run.
    client_t *client = (client_t *) arg;
    pthread_mutex_lock(&thread_list_mutex);
    if (client == thread_list_head) { // if the client is at the head of list
        thread_list_head = client->next;
        if (client->next != NULL) { // if it was not the only item in the list
            client->next->prev = NULL;
        }
    }
    else if (client->next == NULL) { // if the client is at the end of the list
        client->prev->next = NULL;
    }
    else { // otherwise:
        client->prev->next = client->next;
        client->next->prev = client->prev;
    }
    pthread_mutex_unlock(&thread_list_mutex);
    client_destructor(client);
    pthread_mutex_lock(&server.server_mutex);
    server.num_client_threads--; // remove the client thread
    if (server.num_client_threads == 0) {
        pthread_cond_signal(&server.server_cond); // ensures it is safe to call cleanup
    }
    pthread_mutex_unlock(&server.server_mutex);
    return;
}


/**
 * Code executed by the signal handler thread. Waits for a SIGINT to be sent to the server
 * process and cancels all cilent threads when one arrives.
 *
 * Parameters:
 * - arg: a void pointer to the client_t client
 * 
 * Returns:
 * - NULL if successful
 * **/
void *monitor_signal(void *arg) {
    // TODO: Wait for a SIGINT to be sent to the server process and cancel
    // all client threads when one arrives.
    sigset_t *sig = (sigset_t *) arg;
    int signal;
    while(1) {
    sigwait(sig,&signal); // waits on signal
      if (signal == SIGINT) { // if it receives SIGINT
        pthread_mutex_lock(&thread_list_mutex);
        fprintf(stderr, "SIGINT received, cancelling all clients\n");
        delete_all(); // calls delete all
        pthread_mutex_unlock(&thread_list_mutex);
      } 
    }
    return NULL;
}

/**
 * Creates a thread to handle SIGINT. The thread that this function creates 
 * is the the only thread that ever responds to SIGINT.
 *
 * Returns:
 * - sig: the sighandler_t struct created containing the thread responding to SIGINT
 * **/
sig_handler_t *sig_handler_constructor() {
    // TODO: Create a thread to handle SIGINT. The thread that this function
    // creates should be the ONLY thread that ever responds to SIGINT.
    sig_handler_t *sig;
    if ((sig = malloc(sizeof(sig_handler_t))) == 0) { // allocates space
        perror("malloc");
        exit(1);
    }
    sigemptyset(&sig->set);
    sigaddset(&sig->set, SIGINT);
    int error1;
    if ((error1 = pthread_sigmask(SIG_BLOCK, &sig->set, 0)) != 0) {  // blocks SIGINT
        handle_error_en(error1, "pthread_sigmask");
    }
    int error2;
    if ((error2 = pthread_create(&sig->thread, 0, monitor_signal, &sig->set)) != 0) { // creates thread
        handle_error_en(error2, "pthread_create");
    }
    return sig;
}

/**
 * Frees any resources allocates in sig_handler_constructor. Cancels and joins with the
 * signal handler's thread.
 *
 * Parameters:
 * - sighandler: a pointer to the sig_handler_t struct to destroy
 * **/
void sig_handler_destructor(sig_handler_t *sighandler) {
    // TODO: Free any resources allocated in sig_handler_constructor.
    // Cancel and join with the signal handler's thread.
    int error3;
    if ((error3 = pthread_cancel(sighandler->thread)) != 0) { // cancels the thread
        handle_error_en(error3, "pthread_cancel");
    }
    int error4;
    if ((error4 = pthread_join(sighandler->thread, NULL)) != 0) { // joins with the thread
        handle_error_en(error4, "pthread_join");
    }
    free(sighandler); // frees the struct
}


/**
 * Main function
 * Parameters:
 * - argc: the number of arguments entered into the command line
 * - argv: a pointer to the first element in the command line
 *            arguments array, containing the port number.
 * Returns:
 *  - 0 if successful, 1 if there was an error
 **/
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
    int error5;
    if ((error5 = pthread_sigmask(SIG_BLOCK, &signal, NULL)) != 0) { // blocks SIGPIPE
        handle_error_en(error5, "pthread_sigmask");
    }
    sig_handler_t *sig = sig_handler_constructor();
    pthread_t listener = start_listener(atoi(argv[1]), client_constructor);
    while (1) { // creates REPL
        char buf[BUFLEN];
        memset(buf, '\0', BUFLEN);
        int fd = STDIN_FILENO;
        size_t count = BUFLEN;
        ssize_t to_read;
        to_read = read(fd, buf, count); // reads from input
        if (to_read == -1) {
            perror("error: read");
            return 1;
        }
        else if (to_read == 0) {  // if EOF is reached
            fprintf(stderr, "exiting database\n");
            sig_handler_destructor(sig);
            pthread_mutex_lock(&thread_list_mutex); // locks for thread safety
            accepted = 0; // no longer accepting clients
            delete_all(); // sends cancellation req to all clients, wait for last thread to finish destroying
            pthread_mutex_unlock(&thread_list_mutex);
            pthread_mutex_lock(&server.server_mutex);
            while (server.num_client_threads > 0) { // waits for last thread
                pthread_cond_wait(&server.server_cond, &server.server_mutex);
            }
            pthread_mutex_unlock(&server.server_mutex);
            // pthread_cancel(listener);
            db_cleanup();
            pthread_cancel(listener);
            pthread_join(listener, NULL);
            pthread_exit(NULL); // exits REPL
        }
        buf[to_read] = '\0'; // null-terminates buffer
        if (buf[0] == 's') { // stop command
            client_control_stop();
        }
        else if (buf[0] == 'g') { // go command
            client_control_release();
        }
        else if (buf[0] == 'p') { // print command
            char str[BUFLEN];
            // if (&buf[1] != NULL) {
                sscanf(&buf[1],"%s",str); // tokenizes filename
                db_print(str);
            // }
            // else {
            //     db_print(NULL); // otherwise, print to stdout
            // }
        }
    }
    return 0;
}
