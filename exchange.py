import sys, pika, json

def init_startup_args():
    host = "localhost"

    #validate host argument
    if sys.argv[1] is not None or sys.argv[1].strip():
        return sys.argv[1]

    return host

HOST = init_startup_args()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
channel = connection.channel()

#create queues
channel.queue_declare(queue='orders', durable=True, arguments={'x-queue-type': 'quorum'})
channel.queue_declare(queue='trades', durable=True, arguments={'x-queue-type': 'quorum'})

buy_orders = {}
sell_orders = {}

def transact(ch, method, properties, body):
    data = json.loads(body) #deserialize

    transact_key = None
    transact_type = None
    order_filler = None

    #incoming order is sell
    if data["transaction_type"].lower() == "sell":
        transact_type = "sell"

        if len(buy_orders) > 0:
            for key, value in buy_orders.items(): 
                #there exists an acceptable value so commit transaction
                if data["price"] <= value["price"]:
                    transact_key = key
                    order_filler = data["username"]
                    break

            #there is no acceptable value so log for future transactions
            if transact_key is None:
                sell_orders[data["uuid"]] = data

        #there are no current sell orders  
        else:
            sell_orders[data["uuid"]] = data

    #incoming order is buy
    if data["transaction_type"].lower() == "buy":
        transact_type = "buy"

        if len(sell_orders) > 0:
            for key, value in sell_orders.items(): 
                #there exists an acceptable value so commit transaction
                if data["price"] >= value["price"]:
                    transact_key = key
                    order_filler = data["username"]
                    break
            
            #there is no acceptable value so log for future transactions
            if transact_key is None:
                buy_orders[data["uuid"]] = data

        #there are no current buy orders         
        else:
            buy_orders[data["uuid"]] = data

    #commit transaction
    if (transact_key is not None):
        order = None

        #remove from orders book
        if transact_type == "sell":
            order = buy_orders.pop(transact_key) 
        elif transact_type == "buy":
            order = sell_orders.pop(transact_key) 

        if order is not None:
            order["order_filler"] = order_filler
            ch.basic_publish(exchange="", routing_key="trades", body=json.dumps(order)) #publish trade
            action = "Bought" if transact_type == "buy" else "Sold"
            print(f"[{order['username']}] {action} {order['qty']}x stock @ ${order['price']} (fulfilled by {order_filler})")
            
        transact_key = None #reset key for next incoming transaction

        
#subscribe to orders topic
channel.basic_consume(queue='orders', on_message_callback=transact, auto_ack=True)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
    connection.close()