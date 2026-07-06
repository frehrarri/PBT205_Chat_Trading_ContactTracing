import sys, pika, json, random

#ensure valid startup args. if invalid then default.
def init_startup_args():
    host = 'localhost'
    uid = "Bob"
    move_speed = 3

    #validate host argument
    if len(sys.argv) >= 2:
        host = sys.argv[1]

    #validate uid argument
    if len(sys.argv) >= 3 and sys.argv[2].strip():
        uid = sys.argv[2]

    #validate movespeed argument
    if len(sys.argv) >= 4:
        try:
            speed = int(sys.argv[3])
            if 1 <= speed <= 3:
                move_speed = speed
        except (ValueError):
            print("invalid movespeed - defaulting to 3")

    return host, uid, move_speed

#startup args
HOST, UID, MOVE_SPEED = init_startup_args()

def init_position():
    x = random.randint(0,9)
    y = random.randint(0,9)
    return (x,y)

data = { 
        "uid" : str(UID),
        "move_speed" : MOVE_SPEED,
        "init_position" : init_position(),
        }

payload = json.dumps(data) #serialize

connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
channel = connection.channel()

#publish initial args (uid, move_speed) to the position queue
channel.queue_declare(queue='position', durable=True, arguments={'x-queue-type': 'quorum'})

channel.basic_publish(exchange='', routing_key='position', body=payload)
print(f" [Sent] '{data}'")

connection.close()

