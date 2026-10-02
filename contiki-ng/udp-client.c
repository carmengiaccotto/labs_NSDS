#include "contiki.h"
#include "net/routing/routing.h"
#include "random.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"
#include "sys/log.h"

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT	8765
#define UDP_SERVER_PORT	5678

#define OP_READ  0
#define OP_LOCK  1
#define OP_WRITE 2

#define STATUS_OK    0
#define STATUS_ERROR 1

struct client_msg_t {
  uint8_t op;
  int value;
};

struct server_msg_t {
  uint8_t status;
  int value;
};

static struct simple_udp_connection udp_conn;

#define TEST_INTERVAL (10 * CLOCK_SECOND)

enum test_state {
  TEST_READ_INITIAL,
  TEST_TRY_WRITE_FAIL,
  TEST_GET_LOCK,
  TEST_DO_WRITE,
  TEST_LOCK_TIMEOUT,
  TEST_WRITE_AFTER_TIMEOUT
};

static uint8_t current_state = TEST_READ_INITIAL;
static int my_value = 10; 

PROCESS(udp_client_process, "UDP client");
AUTOSTART_PROCESSES(&udp_client_process);

static void
udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{
  struct server_msg_t *msg = (struct server_msg_t *)data;
  
  if(msg->status == STATUS_OK) {
    LOG_INFO("RESPONSE: OK - Value: %d\n", msg->value);
  } else {
    LOG_INFO("RESPONSE: REFUSED/ERROR - Value: %d\n", msg->value);
  }
}

PROCESS_THREAD(udp_client_process, ev, data)
{
  static struct etimer periodic_timer;
  static uip_ipaddr_t dest_ipaddr;
  static struct client_msg_t msg;
  static clock_time_t wait_time; 

  PROCESS_BEGIN();

  my_value += (linkaddr_node_addr.u8[7]); 

  simple_udp_register(&udp_conn, UDP_CLIENT_PORT, NULL,
                      UDP_SERVER_PORT, udp_rx_callback);

  wait_time = (10 + (linkaddr_node_addr.u8[7] % 5)) * CLOCK_SECOND;
  etimer_set(&periodic_timer, wait_time);
  PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

  while(1) {
    
    wait_time = 1 * CLOCK_SECOND;

    if(NETSTACK_ROUTING.node_is_reachable() && NETSTACK_ROUTING.get_root_ipaddr(&dest_ipaddr)) {
      
      wait_time = TEST_INTERVAL + (random_rand() % (2 * CLOCK_SECOND));

      switch(current_state) {
        
        case TEST_READ_INITIAL:
          LOG_INFO("READ Initial Value\n");
          msg.op = OP_READ;
          msg.value = 0;
          current_state = TEST_TRY_WRITE_FAIL;
          break;

        case TEST_TRY_WRITE_FAIL:
          LOG_INFO("Try WRITE without LOCK\n");
          msg.op = OP_WRITE;
          msg.value = my_value;
          current_state = TEST_GET_LOCK;
          break;

        case TEST_GET_LOCK:
          LOG_INFO("Request LOCK\n");
          msg.op = OP_LOCK;
          msg.value = 0;
          current_state = TEST_DO_WRITE;
          wait_time = 1 * CLOCK_SECOND;
          break;

        case TEST_DO_WRITE:
          LOG_INFO("Perform WRITE with LOCK\n");
          msg.op = OP_WRITE;
          msg.value = my_value + 1; 
          current_state = TEST_LOCK_TIMEOUT;
          break;

        case TEST_LOCK_TIMEOUT:
          LOG_INFO("Request LOCK and WAIT\n");
          msg.op = OP_LOCK;
          msg.value = 0;
          current_state = TEST_WRITE_AFTER_TIMEOUT;
          wait_time = 8 * CLOCK_SECOND;
          break;

        case TEST_WRITE_AFTER_TIMEOUT:
          LOG_INFO("Try WRITE after Timeout\n");
          msg.op = OP_WRITE;
          msg.value = my_value + 5;
          current_state = TEST_READ_INITIAL; 
          break;
      }

      simple_udp_sendto(&udp_conn, &msg, sizeof(msg), &dest_ipaddr);

    }

    etimer_set(&periodic_timer, wait_time);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));
  }

  PROCESS_END();
}