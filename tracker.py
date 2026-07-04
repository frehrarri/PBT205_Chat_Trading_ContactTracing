import pika, sys, os, json

environment = [] #the 'chessboard' matrix contains a set for uids
contact = [] #list of dictionaries to track instances of a person making contact with another [{uid, (x,y)}]
positions = {} #each persons current position {uid,(x,y)}



def position_callback(ch, method, properties, body):
    try:
        print(f" [Received] {body}")
        data = json.loads(body)

        #set initial position if there is no current position
        if (positions.get(data['uid']) is None):
            x, y = data['init_position']
            positions[data['uid']] = (x,y)
            return

        #if the person is already on the board, traverse
        move_person(data['uid'], data['traverse'])

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"[Error] Malformed position message: {body} ({e})")     



def query_callback(ch, method, properties, body):
    try:
        print(f" [Received] {body}")
        data = json.loads(body)
        
        #there is an existing uid in the contact queue so we trace their contacts
        if data['uid'] is not None and contact.get(data['uid']) is not None and data['uid'] != "":
            contacted = trace_contact(data['uid'])
            ch.basic_publish(exchange="", routing_key="query_response", body=json.dumps(contacted))

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"[Error] Malformed position message: {body} ({e})")



def main():
    create_env() #create a 10x10 matrix (scalable by passing x,y vals) 

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    #create queues
    channel.queue_declare(queue='position', durable=True, arguments={'x-queue-type': 'quorum'})
    channel.queue_declare(queue='query', durable=True, arguments={'x-queue-type': 'quorum'})
    channel.queue_declare(queue='query_response', durable=True, arguments={'x-queue-type': 'quorum'})

    #subscribe to position topic
    channel.basic_consume(queue='position', on_message_callback=position_callback, auto_ack=True)

    #subscribe to query topic
    channel.basic_consume(queue='query', on_message_callback=query_callback, auto_ack=True)

    #loop and listen forever (blocks anything after)
    print('[Listening] Waiting for messages... To exit press CTRL+C')
    channel.start_consuming()



#create num of rows/columns for a multi-array environment
def create_env(rows = 10, cols = 10):
    global environment
    environment = [[set() for _ in range(cols)] for _ in range(rows)]


#prevent out of bounds traversal
def validate_boundaries(new_position):
    if new_position[0] > 9 or new_position[0] < 0 or new_position[1] > 9 or new_position[1] < 0:
        return False
    
    return True


#move a person to a new position
def move_person(uid, traverse):
    previous = positions.get(uid)

    if previous is None:
        return
    
    x,y = traverse
    new_position = ((previous[0] + x), (previous[1] + y)) #calculate new position after traversal

    #only traverse if movement is within boundaries
    if(validate_boundaries(new_position)):

        #move to new tile: use previous position and add -1, 0, or 1 to x and y values.
        environment[previous[0]][previous[1]].discard(uid)  #remove user from location in matrix
        positions[uid] = new_position #update previous position
        environment[new_position[0]][new_position[1]].add(uid) #add user to matrix

        #if 2+ people occupy same position, log contact
        existing_residents = environment[new_position[0]][new_position[1]] # persons who already exist at the tile being moved to

        if len(existing_residents) > 1:
            contacted_map = { uid : set()} 

            #loop through occupants and add to map ex. contacted_map = {'Bob' : {"Alice", "John"}} 
            for resident in existing_residents:
                if resident != uid:
                    contacted_map[uid].add(resident)

            log_contact(contacted_map, uid)


# log contact between persons
def log_contact(contacted_map, uid):
    contact.append(contacted_map)
    print(f"[Received] Contact between {contacted_map[uid]} and {contacted_map.get(uid)}")


def trace_contact(uid):
    data = contact.get(uid)
    return data


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
