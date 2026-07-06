import sys, pika, json, random, time

BOARD_SIZE = 10

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
    x = random.randint(0, BOARD_SIZE - 1)
    y = random.randint(0, BOARD_SIZE - 1)
    return (x,y)

def move_person():
    x = random.randint(-1,1)
    y = random.randint(-1,1)
    return (x,y)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
channel = connection.channel()

#publish initial args to the position queue
channel.queue_declare(queue='position', durable=True, arguments={'x-queue-type': 'quorum'})

#continually send updated position
try:
    while True:
        time.sleep(MOVE_SPEED)
        data = {
            "uid": str(UID),
            "move_speed" : MOVE_SPEED,
            "init_position" : init_position(),
            "traverse": move_person(),
        }

        payload = json.dumps(data)
        channel.basic_publish(exchange='', routing_key='position', body=payload)
        print(f" [Sent] '{data}'")
    
except KeyboardInterrupt:
    print("Execution interrupted")
finally:
    connection.close()

