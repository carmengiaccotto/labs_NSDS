# Evaluation lab - Akka

## Group members

- Callegari Alessandro Paolo Gianni
- Franchetti-Rosada Alessia
- Giaccotto Carmen

## Description of message flows
The system consists of four main actors: Client, Balancer, Worker0, and Worker1.
Communication follows a structured request/response pattern.

When the ClientActor starts, it receives the reference of the Balancer through a ConfigMsg.
To insert an entry, the client sends a PutMsg to the Balancer using a tell message.
The Balancer receives the PutMsg and, based on the first letter of the name (splitByInitial), decides which worker is primary and which is replica.
It then sends two WorkerMsg messages via tell: one to the primary with copy=false and one to the replica with copy=true.
Each WorkerActor stores the received entry in its primaryAddresses or replicaAddresses map.

When the client sends a GetMsg, it uses ask to query the Balancer.
The Balancer forwards the query to the primary worker (via ask); if it times out, it retries with the replica.
Each WorkerActor, if awake, replies with a ReplyMsg containing the email.
If both workers are sleeping, the Balancer returns a TimeoutMsg to the client.

RestMsg and ResumeMsg control worker behaviour, switching between the active and sleeping states without affecting other actors.
