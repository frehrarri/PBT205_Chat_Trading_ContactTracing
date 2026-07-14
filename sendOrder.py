import sys, pika, json, uuid

def init_startup_args():
    host = "localhost"
    username = None
    transaction_type = None
    price = None
    qty = 100

    #validate host argument
    if sys.argv[1] is not None and sys.argv[1].strip():
        host = sys.argv[1]

    #validate username
    if not sys.argv[2] and not sys.argv[2].strip():
        print("invalid startup arg: username")
        sys.exit(1)
    else:
        username = sys.argv[2]

    #validate SIDE TRANSACTION_TYPE (buy/sell)
    if sys.argv[3].lower() != "buy" and sys.argv[3].lower() != "sell":
        print("invalid startup arg: transaction_type (buy/sell)")
        sys.exit(1)
    else:
        transaction_type = sys.argv[3]

    #validate price
    if sys.argv[4] is not None or sys.argv[4].strip():
        try:
            price = float(sys.argv[4])
        except:
            print("invalid startup arg: price")
            sys.exit(1)

    #validiate quantity
    if sys.argv[5] is not None or sys.argv[5].strip():
        try:
            qty = int(sys.argv[5])
        except:
            qty = 100

        
    return host,  username, transaction_type, price, qty

HOST, USERNAME, TRANSACTION_TYPE, PRICE, QTY = init_startup_args()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
channel = connection.channel()

#create queues
channel.queue_declare(queue='orders', durable=True, arguments={'x-queue-type': 'quorum'})
channel.queue_declare(queue='trades', durable=True, arguments={'x-queue-type': 'quorum'})


#submit a transaction to the orders topic
def submit():
    data = {
            "uuid" : str(uuid.uuid4()),
            "username": USERNAME,
            "transaction_type" : TRANSACTION_TYPE,
            "price" : PRICE,
            "qty": QTY,
        }

    payload = json.dumps(data)
    channel.basic_publish(exchange='', routing_key='orders', body=payload)
    print(f" [{USERNAME}]: {TRANSACTION_TYPE} request {QTY}x stock @ ${PRICE} submitted")

submit()
connection.close()
sys.exit()