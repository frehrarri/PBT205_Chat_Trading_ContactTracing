import pika, json, random, time

uid = "Bob"
MOVE_SPEED = 3 #how many seconds to wait before moving

def init_position():
    x = random.randint(0,9)
    y = random.randint(0,9)
    return (x,y)

#randomly move 0-1 tiles in any direction
def move_person():
    x = random.randint(-1,1)
    y = random.randint(-1,1)
    return (x,y)

data = { 
        "uid" : str(uid),
        "move_speed" : MOVE_SPEED,
        "init_position" : init_position(),
        "traverse" : move_person()
        }

payload = json.dumps(data)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#publish initial args (uid, move_speed) to the position queue
channel.queue_declare(queue='position', durable=True, arguments={'x-queue-type': 'quorum'})

#delay publish to traverse only after the allotted time has expired
time.sleep(MOVE_SPEED) 
channel.basic_publish(exchange='', routing_key='position', body=payload)
print(f" [Sent] '{data}'")

connection.close()

