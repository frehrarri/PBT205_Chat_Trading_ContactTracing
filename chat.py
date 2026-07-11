"""
chat.py - Command-line chat client for RabbitMQ (PBT205 Task 1)

Usage:
    python chat.py --username Alice --endpoint localhost:5672
    python chat.py --username Bob   --endpoint localhost:5672
"""

import argparse
import json
import sys
import threading

import pika

EXCHANGE_NAME = 'room'
EXCHANGE_TYPE = 'fanout'


def parse_args():
    parser = argparse.ArgumentParser(description='Command-line chat client for RabbitMQ')
    parser.add_argument('--username', required=True, help='Your display name in the chat room')
    parser.add_argument('--endpoint', default='localhost:5672',
                         help='Middleware endpoint as host:port (default: localhost:5672)')
    return parser.parse_args()


def connect(endpoint):
    host, _, port = endpoint.partition(':')
    port = int(port) if port else 5672
    # A generous heartbeat, since the main thread blocks on input() while
    # waiting for you to type — a short heartbeat would get missed and
    # RabbitMQ would (silently) drop the connection while you're mid-typing.
    params = pika.ConnectionParameters(host=host, port=port, heartbeat=600)
    return pika.BlockingConnection(params)


def consume_loop(endpoint, username):
    """Runs in a background thread: subscribes to the room and prints incoming messages."""
    connection = connect(endpoint)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    # Each client gets its own exclusive queue bound to the fanout exchange,
    # so every participant receives every message posted to the room.
    result = channel.queue_declare(queue='', exclusive=True, auto_delete=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)

    def on_message(ch, method, properties, body):
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return
        sender = data.get('username', 'unknown')
        text = data.get('text', '')
        if sender != username:  # only print messages posted by OTHER users
            print(f'\n[{sender}] {text}\n> ', end='', flush=True)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=True)

    try:
        channel.start_consuming()
    except (pika.exceptions.ConnectionClosed, pika.exceptions.StreamLostError):
        pass


def publish_loop(endpoint, username):
    """Runs in the main thread: reads stdin and publishes each line to the room."""
    connection = connect(endpoint)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    print(f"Connected as '{username}'. Type a message and press Enter to send. Ctrl+C to quit.")
    try:
        while True:
            text = input('> ')
            if not text:
                continue
            payload = json.dumps({'username': username, 'text': text})
            try:
                channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='', body=payload)
            except (pika.exceptions.StreamLostError,
                    pika.exceptions.ConnectionClosed,
                    pika.exceptions.AMQPConnectionError):
                # The connection went stale (e.g. sat idle too long). Reconnect
                # once and retry sending this same message.
                print('(connection dropped, reconnecting...)')
                connection = connect(endpoint)
                channel = connection.channel()
                channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)
                channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='', body=payload)
    except (KeyboardInterrupt, EOFError):
        print('\nDisconnecting...')
    finally:
        connection.close()


def main():
    args = parse_args()

    # Consuming blocks forever, so it runs in its own daemon thread while the
    # main thread stays free to block on input() for publishing.
    consumer_thread = threading.Thread(
        target=consume_loop, args=(args.endpoint, args.username), daemon=True
    )
    consumer_thread.start()

    publish_loop(args.endpoint, args.username)
    sys.exit(0)


if __name__ == '__main__':
    main()
