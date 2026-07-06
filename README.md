# PBT205_Chat_Trading_ContactTracing
Middleware (Docker w/ RabbitMQ) with 3 prototypes: chatting app, trading system app, contact tracing app.

## ContactTracing:
query.py
- args: python file_name host_address person
- ex: python query.py localhost Bob

person.py
- args: python file_name host_address person seconds_until_move
- ex: python person.py localhost Alice 2

tracker.py
- args: python file_name host_address
- ex: python tracker.py