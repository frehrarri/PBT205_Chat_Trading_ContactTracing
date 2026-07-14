# PBT205_Chat_Trading_ContactTracing
Middleware (Docker w/ RabbitMQ) with 3 prototypes: chatting app, trading system app, contact tracing app.

## ChatRooms
chat.py:
- args: python file_name --username username --endpoint host
- ex: python chat.py --username Alice --endpoint localhost:5672

chat_gui.py:
- args: python file_name
- ex: python chat_gui.py

## StockTrading
sendOrder.py
- args: python file_name host_address username side(transaction_type) price quantity(defaults to 100)
- ex: python sendOrder.py localhost mitch sell 2.48 100

exchange.py
- args: python file_name host_address
- ex: python exchange.py localhost

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