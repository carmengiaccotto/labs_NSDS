#include "contiki.h"
#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"
#include "sys/log.h"
#include "sys/ctimer.h"

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define WITH_SERVER_REPLY  1
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

static int shared_data = 0;

static bool is_locked = false;
static uip_ipaddr_t lock_owner;
static struct ctimer lock_timer;

#define LOCK_TIMEOUT (5 * CLOCK_SECOND)

PROCESS(udp_server_process, "UDP server");
AUTOSTART_PROCESSES(&udp_server_process);

static void
unlock_callback(void *ptr)
{
  if(is_locked) {
    LOG_INFO("Lock TIMEOUT! Releasing lock from ");
    LOG_INFO_6ADDR(&lock_owner);
    LOG_INFO_("\n");
    is_locked = false;
  }
}

static void
udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{

  struct client_msg_t *msg = (struct client_msg_t *)data;
  struct server_msg_t reply;
  
  LOG_INFO("Request OP=%u Val=%d from ", msg->op, msg->value);
  LOG_INFO_6ADDR(sender_addr);
  LOG_INFO_("\n");

  switch(msg->op) {
    
    case OP_READ:
      reply.status = STATUS_OK;
      reply.value = shared_data;
      LOG_INFO("Sending READ value: %d\n", shared_data);
      simple_udp_sendto(&udp_conn, &reply, sizeof(reply), sender_addr);
      break;

    case OP_LOCK:
      if(!is_locked) {
        is_locked = true;
        uip_ipaddr_copy(&lock_owner, sender_addr);
        ctimer_set(&lock_timer, LOCK_TIMEOUT, unlock_callback, NULL);
        reply.status = STATUS_OK;
        reply.value = 0;
        LOG_INFO("Lock ALLOWED to ");
        LOG_INFO_6ADDR(sender_addr);
        LOG_INFO_("\n");
        simple_udp_sendto(&udp_conn, &reply, sizeof(reply), sender_addr);
      } else {
        LOG_INFO("Lock IGNORED (Already locked by ");
        LOG_INFO_6ADDR(&lock_owner);
        LOG_INFO_(")\n");
      }
      break;

    case OP_WRITE:
      if(is_locked && uip_ipaddr_cmp(&lock_owner, sender_addr)) {
        shared_data = msg->value;
        
        is_locked = false;
        ctimer_stop(&lock_timer);

        reply.status = STATUS_OK;
        reply.value = shared_data;
        LOG_INFO("Write SUCCESS (New Val: %d). Lock released.\n", shared_data);
        simple_udp_sendto(&udp_conn, &reply, sizeof(reply), sender_addr);
      } else {
        reply.status = STATUS_ERROR;
        reply.value = shared_data;
        LOG_INFO("Write REFUSED (Not owner or not locked)\n");
        simple_udp_sendto(&udp_conn, &reply, sizeof(reply), sender_addr);
      }
      break;
      
  }
}

PROCESS_THREAD(udp_server_process, ev, data)
{
  PROCESS_BEGIN();

  NETSTACK_ROUTING.root_start();

  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);

  PROCESS_END();
}