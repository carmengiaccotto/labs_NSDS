# Evaluation lab - Contiki-NG

## Group members

- Callegari Alessandro Paolo Gianni
- Franchetti-Rosada Alessia
- Giaccotto Carmen

## Solution description

Our solution implements a networked data structure managed by a server and accessed by remote clients via UDP. The system supports three operations: READ, LOCK and WRITE.

**Server Logic:**
The server (`udp-server.c`) maintains a `shared_data` integer (Line 34). It enforces locking logic using a `lock_owner` address and a `ctimer` for timeouts.
- **READ:** always accepted (Lines 75-80).
- **LOCK:** allowed only if `is_locked` is false. It sets a 5 second timeout (Lines 83-92). If already locked the request is ignored (Lines 93-96).
- **WRITE:** permitted only if the sender matches `lock_owner`. A successful write updates the value and immediately releases the lock (Lines 101-110).

**Client Logic:**
The client (`udp-client.c`) implements a state machine (`test_state`) to validate the protocol. To avoid collisions it uses a deterministic startup delay derived from its Node ID (Line 82). After acquiring a lock in state `TEST_GET_LOCK`, the client waits only 1 second (Line 115) to ensure the write operation occurs before the server's 5 second timeout expires.

## Code Reference & Behavior

**Server (`udp-server.c`)**
- **Line 40 (`LOCK_TIMEOUT`):** defines the lock duration as 5 seconds.
- **Lines 45-54 (`unlock_callback`):** automatically releases the lock when the timer expires.
- **Line 85 (`uip_ipaddr_copy`):** stores the IPv6 address of the client acquiring the lock.
- **Line 101 (`uip_ipaddr_cmp`):** verifies if the client attempting to WRITE is the current lock owner.
- **Line 105 (`ctimer_stop`):** stops the timeout timer immediately after a successful WRITE.

**Client (`udp-client.c`)**
- **Line 77 & 82 (`linkaddr_node_addr.u8[7]`):** extracts the unique Node ID from the MAC address. We explicitly use this instead of `random_rand()` to ensure deterministic behavior. This guarantees that test sequences (values and timing offsets) are perfectly reproducible across simulations, aiding debugging while still preventing collisions.
- **Line 115 (`TEST_GET_LOCK`):** sets `wait_time` to 1 second. This ensures the client sends the WRITE request quickly, while the lock is still valid.
- **Line 130 (`TEST_LOCK_TIMEOUT`):** sets `wait_time` to 8 seconds. This forces the client to wait longer than the lock duration (5s), verifying that the server correctly revokes the lock and refuses subsequent writes.