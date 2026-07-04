import pika, json

uid = 'Alice'

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='query', durable=True, arguments={'x-queue-type': 'quorum'})

channel.basic_publish(exchange='', routing_key='query', body=uid)
print(f" [Sent] '{uid}'")

def query_response_callback(ch, method, properties, body):
    try:
        print(f" [x] Received {body}")
        contacted = json.loads(body)
        for contacts in reversed(contacted):
            print(contacts)

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"[Error] Malformed message: {body} ({e})")


#subscribe to query_response topic
channel.basic_consume(queue='query_response', on_message_callback=query_response_callback(), auto_ack=True)

#loop and listen forever (blocks anything after)
print('[Listening] Waiting for messages... To exit press CTRL+C')
channel.start_consuming()

# connection.close()