## Summary

This project implements a simplified TCP-like protocol over UDP, adding connection establishment through a three-way handshake and a sliding window with retransmissions.

The connection-oriented part is implemented on top of an existing UDP-based communication layer. The protocol uses sequence numbers, acknowledgements, timers, and sliding windows to provide reliable data transfer.

## About `lib.h`

The `lib.h` file contains the connection structure used by both the sender and receiver. It contains the UDP socket associated with the connection, the connection identifier, the destination address, and a mutex used for synchronization.

The sender-related and receiver-related fields are separated as follows:

### For both sender and receiver

* `buffer`: This buffer is used on both sides as an internal segment buffer. On the sender side, it stores packets that have been created and need to be sent. On the receiver side, it stores received payloads indexed by their sequence number until they can be delivered in order.

### For sender

* `start_unconfirmed`: The first segment that has been sent but not yet acknowledged.

* `next_seq`: The next sequence number that will be assigned to a new data segment.

* `next_to_send`: The next segment from the internal buffer that has not yet been sent for the first time.

* `send_bytes`: The number of bytes currently stored in the sender buffer.

* `max_window_seq`: The maximum number of segments allowed in the sender window.

### For receiver

* `expected`: The next sequence number expected by the receiver.

* `data_ok`: Data that has already been received in the correct order and is ready to be returned to the application.

* `recv_bytes`: The number of bytes currently stored in the receiver buffers.

* `cap`: The capacity of the receiver buffer.

* `recv_ok`: A condition variable used to wake up `recv_data` when new data becomes available.

## About `libsend.cpp`

The sender is initialized with `init_sender(speed, delay)`. This function calculates an approximation of the window size using the network speed and bandwidth. The result is converted into a number of segments, assuming maximum-size segments, and stored in `MAX_WIN_SIZE`. A background handler thread is then started.

The connection is established using `setup_connection`, which implements the client side of the three-way handshake. First, a SYN packet is sent to the receiver's listening port. The sender then waits for a SYN-ACK response. The SYN-ACK also contains the new UDP port selected for data transfer. After receiving this port, the sender updates the destination address and sends the final ACK.

A timer is then created and the connection is inserted into the global connection map. A loop is used to make sure that the connection is established correctly and that unrelated or invalid messages are not interpreted as handshake packets.

Data is sent using `send_data`. The function splits the data into chunks of at most `MAX_DATA_SIZE`. For each chunk, it creates a `poli_tcp_data_hdr` header, fills in its fields, and copies the header and payload into a vector.

The vector is stored in the connection buffer using the sequence number as a key. After adding the data, `send_data` calls `send_first_win` to send the packets that have not been transmitted before. Packets are sent while the current position is inside the sending window, preventing the sender from flooding the network with too many unacknowledged packets.

The sender handler processes ACKs. When an ACK is received, the handler extracts its sequence number. Since the ACKs are cumulative, an ACK with number `k` indicates that all packets with sequence numbers smaller than `k` have been received in order.

The acknowledged packets are then removed from the buffer and `send_bytes` is updated. The function subsequently calls `send_first_win` again to send new packets that can enter the window.

If the handler receives a timeout event instead of an ACK, it calls `resend_win`. This retransmits the packets currently inside the sending window that have already been sent at least once. This behavior is similar to a Selective Repeat protocol.

## About `librecv.cpp`

The receiver is initialized with `init_receiver(recv_buffer_bytes)`. This function stores the receiver buffer capacity, creates a UDP socket, binds it to port `8032`, and starts the receiver handler thread.

A server accepts a connection using `wait4connect`. The function waits for a SYN packet on the listening socket. When a valid SYN is received, a new UDP socket is created for the client and bound to a randomly selected free port.

The receiver then sends a SYN-ACK containing the control header and the new port. The SYN-ACK is repeatedly sent until the final ACK is received from the sender. After that, the connection is considered established and is registered.

Loops are used during the handshake to make sure the connection is established correctly without accidentally processing unrelated or invalid packets.

The receiver handler runs in a separate thread. It waits for incoming data using `recv_message_or_timeout`. When a segment is received, it locks the connection mutex and validates the packet by checking the protocol ID, packet type, and payload length. Invalid packets are ignored.

If the sequence number is smaller than `expected`, the segment is considered a duplicate or an old retransmission. An ACK is sent and the packet is ignored. Sending the ACK is necessary because this situation can occur when a previous acknowledgement was lost and the sender retransmits an already received segment.

If the segment is new and there is enough space in the receiver buffer, its payload is copied into the connection buffer using `seq` as the key.

After storing a segment, the receiver checks whether the segment with the currently expected sequence number is available in the buffer. While `buffer[expected]` exists, its payload is moved into `data_ok`. This ensures that the application receives the data in the correct order, even when segments arrive out of order. The segment is then removed from the buffer and `expected` is incremented.

When `data_ok` contains data, the receiver signals the `recv_ok` condition variable.

If no data is ready, `recv_data` waits on the condition variable. Once data becomes available, it copies at most `len` bytes from `data_ok` into the application buffer, removes those bytes from `data_ok`, updates `recv_bytes`, and sends an ACK.

## Synchronization

Both the sender and receiver use handler threads. To prevent concurrent access to shared resources, each connection has its own mutex.

The mutex protects shared fields such as the segment buffer, byte counters, sequence numbers, and ready data buffer.

The receiver also uses a condition variable. When no data is ready, `recv_data` waits on the condition variable. When new data is added to `data_ok`, the condition variable is signaled and the waiting thread is awakened.

## Overview

The project demonstrates the main mechanisms required for reliable, connection-oriented communication over UDP, including three-way connection establishment, sequence numbers, cumulative acknowledgements, sliding windows, retransmissions, timers, and thread synchronization.

The implementation focuses on the core concepts behind TCP while keeping the protocol significantly simpler than a full TCP implementation.
