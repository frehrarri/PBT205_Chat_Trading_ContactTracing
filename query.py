import pika

uid = 'Alice'

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='query', durable=True, arguments={'x-queue-type': 'quorum'})
channel.basic_publish(exchange='', routing_key='query', body=uid)
print(f" [x] Sent '{uid}'")

connection.close()