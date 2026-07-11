"""
chat_gui.py - GUI chat client with multiple rooms (PBT205 Task 1 - Final Product)

This extends the base command-line prototype (chat.py) to satisfy the
"Final Product" requirements:
  - A simple GUI where a user logs in and joins a room.
  - Support for multiple rooms, chosen at login.

Run:
    python chat_gui.py
"""

import json
import queue
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

import pika

EXCHANGE_NAME = 'chat_rooms'
EXCHANGE_TYPE = 'topic'   # routing key = room name, so each room is isolated


class ChatClient:
    """Owns the RabbitMQ connections. Kept separate from the GUI code so the
    messaging logic can be tested/reasoned about independently of tkinter."""

    def __init__(self, endpoint, username, room, incoming_queue):
        self.endpoint = endpoint
        self.username = username
        self.room = room
        self.incoming_queue = incoming_queue  # thread-safe hand-off to the GUI
        self.publish_connection = None
        self.publish_channel = None

    def _connect(self):
        host, _, port = self.endpoint.partition(':')
        port = int(port) if port else 5672
        params = pika.ConnectionParameters(host=host, port=port, heartbeat=600)
        return pika.BlockingConnection(params)

    def start(self):
        # Connection for publishing lives on the GUI thread (it's only used
        # briefly, on button clicks, so it won't freeze the interface).
        self.publish_connection = self._connect()
        self.publish_channel = self.publish_connection.channel()
        self.publish_channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

        threading.Thread(target=self._consume, daemon=True).start()

    def _consume(self):
        """Runs on a background thread: subscribes to this room and pushes
        incoming messages onto a queue the GUI thread polls safely."""
        connection = self._connect()
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

        result = channel.queue_declare(queue='', exclusive=True, auto_delete=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=self.room)

        def on_message(ch, method, properties, body):
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return
            if data.get('username') != self.username:
                self.incoming_queue.put(data)

        channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=True)
        try:
            channel.start_consuming()
        except (pika.exceptions.ConnectionClosed, pika.exceptions.StreamLostError):
            pass

    def send(self, text):
        payload = json.dumps({'username': self.username, 'text': text})
        try:
            self.publish_channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=self.room, body=payload)
        except (pika.exceptions.StreamLostError,
                pika.exceptions.ConnectionClosed,
                pika.exceptions.AMQPConnectionError):
            self.publish_connection = self._connect()
            self.publish_channel = self.publish_connection.channel()
            self.publish_channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)
            self.publish_channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=self.room, body=payload)


class ChatApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('RabbitMQ Chat')
        self.geometry('420x520')
        self.incoming_queue = queue.Queue()
        self.client = None
        self._build_login_screen()

    # ---------- Login screen: username + room + endpoint ----------
    def _build_login_screen(self):
        self.login_frame = ttk.Frame(self, padding=20)
        self.login_frame.pack(expand=True)

        ttk.Label(self.login_frame, text='Username').grid(row=0, column=0, sticky='w', pady=5)
        self.username_entry = ttk.Entry(self.login_frame, width=25)
        self.username_entry.grid(row=0, column=1, pady=5)

        ttk.Label(self.login_frame, text='Room').grid(row=1, column=0, sticky='w', pady=5)
        self.room_entry = ttk.Entry(self.login_frame, width=25)
        self.room_entry.insert(0, 'general')
        self.room_entry.grid(row=1, column=1, pady=5)

        ttk.Label(self.login_frame, text='Endpoint').grid(row=2, column=0, sticky='w', pady=5)
        self.endpoint_entry = ttk.Entry(self.login_frame, width=25)
        self.endpoint_entry.insert(0, 'localhost:5672')
        self.endpoint_entry.grid(row=2, column=1, pady=5)

        ttk.Button(self.login_frame, text='Join', command=self._on_join).grid(
            row=3, column=0, columnspan=2, pady=15
        )
        self.username_entry.focus()

    def _on_join(self):
        username = self.username_entry.get().strip()
        room = self.room_entry.get().strip()
        endpoint = self.endpoint_entry.get().strip()

        if not username or not room or not endpoint:
            messagebox.showwarning('Missing info', 'Please fill in username, room, and endpoint.')
            return

        try:
            self.client = ChatClient(endpoint, username, room, self.incoming_queue)
            self.client.start()
        except Exception as exc:
            messagebox.showerror('Connection failed', f'Could not connect to RabbitMQ:\n{exc}')
            return

        self.username = username
        self.room = room
        self.login_frame.destroy()
        self._build_chat_screen()
        self.after(100, self._poll_incoming)

    # ---------- Chat screen ----------
    def _build_chat_screen(self):
        header = ttk.Label(self, text=f'{self.username} — room: {self.room}', font=('Segoe UI', 10, 'bold'))
        header.pack(pady=(10, 0))

        self.messages_box = scrolledtext.ScrolledText(self, state='disabled', wrap='word')
        self.messages_box.pack(fill='both', expand=True, padx=10, pady=10)

        entry_frame = ttk.Frame(self)
        entry_frame.pack(fill='x', padx=10, pady=(0, 10))

        self.message_entry = ttk.Entry(entry_frame)
        self.message_entry.pack(side='left', fill='x', expand=True)
        self.message_entry.bind('<Return>', lambda event: self._on_send())
        self.message_entry.focus()

        ttk.Button(entry_frame, text='Send', command=self._on_send).pack(side='left', padx=(5, 0))

    def _on_send(self):
        text = self.message_entry.get().strip()
        if not text:
            return
        self.client.send(text)
        self._append_message(self.username, text)
        self.message_entry.delete(0, 'end')

    # ---------- Thread-safe hand-off from the consumer thread ----------
    def _poll_incoming(self):
        while not self.incoming_queue.empty():
            data = self.incoming_queue.get_nowait()
            self._append_message(data.get('username', 'unknown'), data.get('text', ''))
        self.after(100, self._poll_incoming)

    def _append_message(self, sender, text):
        self.messages_box.configure(state='normal')
        self.messages_box.insert('end', f'[{sender}] {text}\n')
        self.messages_box.configure(state='disabled')
        self.messages_box.see('end')


if __name__ == '__main__':
    app = ChatApp()
    app.mainloop()
