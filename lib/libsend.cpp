#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <string.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int MAX_WIN_SIZE = 0;

void send_first_win(struct connection *con) {
    while (con->next_to_send < con->next_seq) {
        if(con->next_to_send >= con->start_unconfirmed + con->max_window_seq) {
            break;
        }
        auto idx = con->buffer.find(con->next_to_send);

        if (idx != con->buffer.end()) {
            sendto(con->sockfd, idx->second.data(), idx->second.size(), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
        }

        con->next_to_send++;
    }
}

void resend_win(struct connection *con)
{
    auto idx = con->buffer.begin();

    while (idx != con->buffer.end()) {
        uint16_t seq = idx->first;

        if (seq < con->start_unconfirmed) {
            idx++;
            continue;
        }

        if (seq >= con->start_unconfirmed + con->max_window_seq) {
            break;
        }


        if (seq < con->next_to_send) {
            sendto(con->sockfd, idx->second.data(), idx->second.size(), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
        }

        idx++;
    }
}

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;
    int max = 1024*1024;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    /* We will write code here as to not have sync problems with sender_handler */

    if (cons[conn_id]->send_bytes >= max) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }


    while (size < len) {
        if(cons[conn_id]->send_bytes >= max) {
            break;
        }

        int left = len - size;
        int curr_len;

        if (left > MAX_DATA_SIZE) {
            curr_len = MAX_DATA_SIZE;
        } else {
            curr_len = left;
        }

        if (cons[conn_id]->send_bytes + curr_len > max) {
            break;
        }

        struct poli_tcp_data_hdr hdr;
        hdr.protocol_id = POLI_PROTOCOL_ID;
        hdr.conn_id = cons[conn_id]->conn_id;
        hdr.type = 3;
        hdr.seq_num = htons(cons[conn_id]->next_seq);
        hdr.len = htons(curr_len);

        int packet_len = sizeof(struct poli_tcp_data_hdr) + curr_len;
        vector<char> packet(packet_len);

        memcpy(packet.data(), &hdr, sizeof(hdr));
        memcpy(packet.data() + sizeof(hdr), buffer + size, curr_len);

        cons[conn_id]->buffer[cons[conn_id]->next_seq] = packet;

        cons[conn_id]->next_seq++;
        cons[conn_id]->send_bytes += curr_len;
        size += curr_len;
    }

    if(size > 0) {
        send_first_win(cons[conn_id]);
    }

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    if (size == 0) {
        return -1;
    }

    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];


    while (1) {

        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        if(res < 0) {
            resend_win(cons[conn_id]);
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }
        struct poli_tcp_ctrl_hdr *received = (struct poli_tcp_ctrl_hdr *)buf;

        if (received->protocol_id == POLI_PROTOCOL_ID && received->type == 2) {
            uint16_t ack = ntohs(received->ack_num);

            while (!cons[conn_id]->buffer.empty()) {
                if (cons[conn_id]->buffer.begin()->first < ack) {
                    int size = cons[conn_id]->buffer.begin()->second.size() - sizeof(struct poli_tcp_data_hdr);
                    cons[conn_id]->send_bytes -= size;
                    cons[conn_id]->buffer.erase(cons[conn_id]->buffer.begin());
                    cons[conn_id]->start_unconfirmed = ack;
                } else {
                    break;
                }
            }

            send_first_win(cons[conn_id]);
        }
        
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct sockaddr_in servaddr;
    servaddr.sin_addr.s_addr = ip;
    servaddr.sin_port = port;
    servaddr.sin_family = AF_INET;

    struct connection *con = new connection();
    static int next_id = 0;
    int conn_id = next_id++;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    con->servaddr = servaddr;
    con->conn_id = conn_id;
    con->start_unconfirmed = 0;
    con->next_seq = 0;
    con->next_to_send = 0;
    con->expected = 0;
    con->max_window_seq = MAX_WIN_SIZE;
    con->send_bytes = 0;
    con->recv_bytes = 0;
    con->cap = 0;
    

    struct poli_tcp_ctrl_hdr packet;
    packet.type = 0;
    packet.ack_num = 0;
    packet.recv_window = MAX_WIN_SIZE;
    packet.protocol_id = POLI_PROTOCOL_ID;

    // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    }

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    while(1) {
        sendto(con->sockfd, &packet, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&servaddr, sizeof(servaddr));

        char buf[100];
        struct poli_tcp_ctrl_hdr received;
        int len = sizeof(servaddr);
        int rc = recvfrom(con->sockfd, buf, sizeof(buf), 0, (struct sockaddr *)&servaddr, (socklen_t *)&len);
        if(rc < 0) {
            perror("Error");
            continue;
        }
        memcpy(&received, buf, sizeof(struct poli_tcp_ctrl_hdr));

        if(received.protocol_id == POLI_PROTOCOL_ID && received.type == 1) {
            uint16_t aux;
            memcpy(&aux, buf + sizeof(struct poli_tcp_ctrl_hdr), sizeof(uint16_t));
            con->servaddr.sin_port = aux;
            break;
        } else {
            continue;
        }
    }

     struct poli_tcp_ctrl_hdr ack;
     ack.protocol_id = POLI_PROTOCOL_ID;
     ack.conn_id = conn_id;
     ack.type = 2;
     ack.ack_num = 0;
     ack.recv_window = MAX_WIN_SIZE;

     sendto(con->sockfd, &ack, sizeof(struct poli_tcp_ctrl_hdr), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

     tv.tv_sec = 0;
     tv.tv_usec = 0;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
       perror("Error");
    }


    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 20000000;

    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 20000000;   
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;


    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;


    double total_bits = speed * 1000.0 * (2*delay);
    double dim_bits = MAX_SEGMENT_SIZE * 8.0;

    int max_window = (int)(total_bits / dim_bits) * 2;

    if(max_window < 1) {
        max_window = 1;
    }

    MAX_WIN_SIZE = max_window;


    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);

}
