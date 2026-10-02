# Evaluation lab - Node-RED

## Group members

- Carmen Giaccotto
- Alessia Franchetti-Rosada
- Alessandro Callegari

## Bot URL
`****************`

## Description of message flows
The solution handles Telegram messages starting with specific commands. The Telegram Receiver passes the incoming message to the Parser function node. This node parses the command (QUERY, TRACK, REPORT) and routes the message to one of three outputs.

1. QUERY (Output 1): The message is sent to the openweathermap node to fetch real-time data. The result is passed to a Text node (labeled "Output") for formatting and finally to the Telegram Sender.

2. TRACK (Output 2): The message reaches the Save function, which stores the user's location preference in the global context. A confirmation message is formatted by a Text node and sent to the Telegram Sender.

3. REPORT (Output 3): The Iterator function retrieves the user's saved locations and generates a sequence of messages. Each message triggers the openweathermap node. The resulting single-location forecasts are formatted by a Text node and aggregated into a single string by the Join node before being sent to the Telegram Sender.

## Extensions 

We only used extensions seen during the regular labs:

1. node-red-node-openweathermap

2. node-red-contrib-chatbot
