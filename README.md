# Networked Software for Distributed Systems A.Y. 2025-2026
## Computer Science and Engineering

This repository contains our solutions to the evaluation labs of the course "Networked Software for Distributed Systems" at [Politecnico di Milano](https://www.polimi.it).

### Covered Topics and Technologies

The labs cover several key technologies for developing networked and distributed systems. Each folder contains the solution to one evaluation lab:

| Folder | Topic | Technology | Solution |
|---|---|---|---|
| [akka/](akka/) | Actor model | Akka (Java) | Replicated address book: a balancer splits entries between two workers, each keeping a primary and a replica copy, and falls back to the replica when the primary is resting |
| [kafka/](kafka/) | Distributed message queues | Apache Kafka (Java) | Two-stage pipeline: a *Merger* that combines the values of two sensor topics and a transactional *Validator* that forwards them exactly once to two output topics |
| [spark/](spark/) | Big data processing | Apache Spark (Java) | Product analytics: batch queries on historical purchases (top products, revenue per category) and windowed streaming queries on live purchases |
| [contiki-ng/](contiki-ng/) | Internet of Things (IoT) | Contiki-NG (C) | UDP server exposing a shared value with READ, LOCK and WRITE operations, lock timeout, and a test client simulated in Cooja |
| [node-red/](node-red/) | Computing Continuum | Node-RED | Telegram bot that answers weather queries through OpenWeatherMap and reports on the locations each user tracks |
| [mpi/](mpi/) | Parallel and distributed computing | MPI (C) | Counts the local minima of each row of a matrix whose rows are distributed across processes, exchanging border rows with neighbours |

The folders `akka/`, `contiki-ng/` and `node-red/` include a README describing the solution in detail.

### Acknowledgements

Each evaluation lab started from a code skeleton provided by the course instructors (e.g. the main classes, part of the message classes and the input data generation). The Contiki-NG code is based on the `rpl-udp` example of [Contiki-NG](https://github.com/contiki-ng/contiki-ng). The rest of the code is our own work.

### List of Authors:
 - *[Carmen Giaccotto](https://github.com/carmengiaccotto)*
 - *[Alessia Franchetti-Rosada](https://github.com/alessiafranchetti)*
 - *[Alessandro Callegari](https://github.com/Ale02014)*
