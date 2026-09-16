#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <string.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int listenfd;

int MAX_WIN_SIZE;

void send_ack(struct connection *con)
{
    struct poli_tcp_ctrl_hdr ack;
    memset(&ack, 0, sizeof(ack));
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = con->conn_id;
    ack.type = 2;
    ack.ack_num = htons(con->expected);

    int remaining = con->cap - con->recv_bytes;
    if (remaining < 0) {
        remaining = 0;
    }

    ack.recv_window = htons(remaining);

    sendto(con->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

int recv_data(int conn_id, char *buffer, int len)
{
    pthread_mutex_lock(&cons[conn_id]->con_lock);
    /* We will write code here as to not have sync problems with recv_handler */

    while (cons[conn_id]->data_ok.empty()) {
        pthread_cond_wait(&cons[conn_id]->recv_ok, &cons[conn_id]->con_lock);
    }

    int size = (int) cons[conn_id]->data_ok.size();
    if (len < (int) cons[conn_id]->data_ok.size()) {
        size = len;
    }

    memcpy(buffer, cons[conn_id]->data_ok.data(), size);

    cons[conn_id]->data_ok.erase(cons[conn_id]->data_ok.begin(), cons[conn_id]->data_ok.begin() + size);
    cons[conn_id]->recv_bytes -= size;

    if (cons[conn_id]->recv_bytes < 0) {
        cons[conn_id]->recv_bytes = 0;
    }

    if(size > 0) {
        send_ack(cons[conn_id]);
    }

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}


void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        if (conn_id < 0 || cons.find(conn_id) == cons.end()) {
            continue;
        }

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */


        if(res < 0) {
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }

        poli_tcp_data_hdr *ok = (struct poli_tcp_data_hdr *) segment;

        int len = ntohs(ok->len);
        if((ok->protocol_id != POLI_PROTOCOL_ID) || (ok->type != 3) || (len > MAX_DATA_SIZE)) {
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }

        int curr_seq = ntohs(ok->seq_num);
        if(curr_seq < cons[conn_id]->expected) {
            send_ack(cons[conn_id]);
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }

        if(cons[conn_id]->cap - cons[conn_id]->recv_bytes >= len && cons[conn_id]->buffer.find(curr_seq) == cons[conn_id]->buffer.end()) {
            vector<char> aux(len);
            memcpy(aux.data(), segment + sizeof(struct poli_tcp_data_hdr), len);

            cons[conn_id]->buffer[curr_seq] = aux;
            cons[conn_id]->recv_bytes += len;
            
        }


        while (cons[conn_id]->buffer.find(cons[conn_id]->expected) != cons[conn_id]->buffer.end()) {
            vector<char> aux = cons[conn_id]->buffer[cons[conn_id]->expected];
            for(int i = 0; i < (int)aux.size(); i++) {
                cons[conn_id]->data_ok.push_back(aux[i]);
            }

            cons[conn_id]->buffer.erase(cons[conn_id]->expected);
            cons[conn_id]->expected++;
        }

        if (!cons[conn_id]->data_ok.empty()) {
            pthread_cond_signal(&cons[conn_id]->recv_ok);
        }

        send_ack(cons[conn_id]);

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    struct sockaddr_in clientaddr;
    int len = sizeof(clientaddr);
    char buf[100];

    struct connection *con = new connection();
    static int next_id = 0;
    int conn_id = next_id++;

    while(1) {
        int rc = recvfrom(listenfd, buf, sizeof(buf), 0, (struct sockaddr *)&clientaddr, (socklen_t *)&len);

        struct poli_tcp_ctrl_hdr *received = (struct poli_tcp_ctrl_hdr *) buf;

        if(received->protocol_id != POLI_PROTOCOL_ID || received->type != 0) {
            continue;
        }
        break;
    }
    



    /*This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)*/
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 20000000;

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    con->servaddr = clientaddr;
    con->conn_id = conn_id;
    con->start_unconfirmed = 0;
    con->next_seq = 0;
    con->next_to_send = 0;
    con->expected = 0;
    con->max_window_seq = MAX_WIN_SIZE;
    con->send_bytes = 0;
    con->recv_bytes = 0;
    con->cap = MAX_WIN_SIZE;

    pthread_mutex_init(&con->con_lock, NULL);
    pthread_cond_init(&con->recv_ok, NULL);

    struct sockaddr_in curr_addr;
    curr_addr.sin_port = htons(0);
    curr_addr.sin_family = AF_INET;
    curr_addr.sin_addr.s_addr = INADDR_ANY;
    bind(con->sockfd, (struct sockaddr *)&curr_addr, sizeof(curr_addr));


    struct sockaddr_in new_addr;
    int new_len = sizeof(new_addr);
    getsockname(con->sockfd, (struct sockaddr *)&new_addr, (socklen_t *)&new_len);

    char aux_buf[100];
    struct poli_tcp_ctrl_hdr tosend;
    memset(&tosend, 0, sizeof(tosend));
    tosend.protocol_id = POLI_PROTOCOL_ID;
    tosend.type = 1;
    tosend.recv_window = htons(MAX_WIN_SIZE);
    tosend.conn_id = conn_id;
    tosend.ack_num = 0;

    memcpy(aux_buf, &tosend, sizeof(tosend));
    memcpy(aux_buf + sizeof(tosend), &new_addr.sin_port, sizeof(uint16_t));


    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    }


    char ack[100];
    struct sockaddr_in from_addr;
    int from_len = sizeof(from_addr);


    while(1) {
        sendto(listenfd, aux_buf, sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t), 0, (struct sockaddr *)&clientaddr, (socklen_t )len);
        int rc = recvfrom(con->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&from_addr, (socklen_t *)&from_len);
        if(rc < 0) {
            perror("Error");
            continue;
        }

        struct poli_tcp_ctrl_hdr *recvack = (struct poli_tcp_ctrl_hdr *) ack;
        if(recvack->protocol_id == POLI_PROTOCOL_ID && recvack->type == 2) {
            break;
        }
    }
    tv.tv_sec = 0;
    tv.tv_usec = 0;

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */

    
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;    
    spec.it_value.tv_nsec = 20000000;    
    spec.it_interval.tv_sec = 0;    
    spec.it_interval.tv_nsec = 20000000;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;    

    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{

    MAX_WIN_SIZE = recv_buffer_bytes;
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */
    listenfd = socket(AF_INET, SOCK_DGRAM, 0);

    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(8032);

    bind (listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);


}
