import sys, pika, json

#ensure valid startup args. if invalid then default.
def init_startup_args():
    host = 'localhost'
    uid = "Alice"

    #validate host argument
    if len(sys.argv) >= 2:
        host = sys.argv[1]

    #validate uid argument
    if len(sys.argv) >= 3 and sys.argv[2].strip():
        uid = sys.argv[2]

    return host, uid

HOST, UID = init_startup_args()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
channel = connection.channel()

channel.queue_declare(queue='query', durable=True, arguments={'x-queue-type': 'quorum'})
channel.queue_declare(queue='query_response', durable=True, arguments={'x-queue-type': 'quorum'})

payload = json.dumps({"uid" : UID}) #serialize

channel.basic_publish(exchange='', routing_key='query', body=payload)
print(f" [Sent] '{UID}'")

def query_response_callback(ch, method, properties, body):
    try:
        print(f" [Received] {body}")
        contacted = json.loads(body)
        for contacts in reversed(contacted):
            print(contacts)

        ch.stop_consuming()

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"[Error] Malformed message: {body} ({e})")


#subscribe to query_response topic
channel.basic_consume(queue='query_response', on_message_callback=query_response_callback, auto_ack=True)

#loop and listen forever (blocks anything after)
print('[Listening] Waiting for messages... To exit press CTRL+C')
channel.start_consuming()

connection.close()