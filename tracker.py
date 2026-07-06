import pika, sys, os, json, threading
import tkinter as tk

#tkinter variables
CELL_SIZE = 40  # pixels per board square
root = None
canvas = None

BOARD_SIZE = 10
environment = [] #the 'chessboard' matrix contains a set for uids
contact = [] #list of dictionaries to track instances of a person making contact with another [{uid, (x,y)}]
positions = {} #each persons current position {uid,(x,y)}


def init_startup_args():
    host = "localhost"

    #validate host argument
    if len(sys.argv) >= 2:
        host = sys.argv[1]

    return host

#startup args
HOST = init_startup_args()

def position_callback(ch, method, properties, body):
    try:
        print(f" [Received] {body}")
        data = json.loads(body) #deserialize

        #set initial position if there is no current position
        if (positions.get(data['uid']) is None):
            x,y = data['init_position']

            if(validate_boundaries(data['init_position'])):
                positions[data['uid']] = (x,y) #update the position tracker
                environment[x][y].add(data['uid']) # add initial position to board

                #clear and redraw personnel on GUI
                request_redraw()
            return

        #if the person is already on the board, traverse
        move_person(data['uid'], data['traverse'])

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"[Error] Malformed position message: {body} ({e})")     



def query_callback(ch, method, properties, body):
    try:
        print(f" [Received] {body}")
        data = json.loads(body) #deserialize
        
        #there is an existing uid in the contact queue so we trace their contacts
        uid = data['uid']
        if uid:
            contacted = trace_contact(uid)
            names = [list(entry.values())[0][0] for entry in contacted]
            ch.basic_publish(exchange="", routing_key="query_response", body=json.dumps(names))

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"[Error] Malformed position message: {body} ({e})")



def main():
    create_env() #create a 10x10 matrix (scalable by passing x,y vals) 

    #create a new thread to utilize tkinter in the event loop without blocking
    consumer_thread = threading.Thread(target=start_consuming_thread, daemon=True)
    consumer_thread.start()
    start_gui()  

    # connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    # channel = connection.channel()

    # #create queues
    # channel.queue_declare(queue='position', durable=True, arguments={'x-queue-type': 'quorum'})
    # channel.queue_declare(queue='query', durable=True, arguments={'x-queue-type': 'quorum'})
    # channel.queue_declare(queue='query_response', durable=True, arguments={'x-queue-type': 'quorum'})

    # #subscribe to position topic
    # channel.basic_consume(queue='position', on_message_callback=position_callback, auto_ack=True)

    # #subscribe to query topic
    # channel.basic_consume(queue='query', on_message_callback=query_callback, auto_ack=True)

    # #loop and listen forever (blocks anything after)
    # print('[Listening] Waiting for messages... To exit press CTRL+C')
    # channel.start_consuming()



#create num of rows/columns for a multi-array environment
def create_env(rows = BOARD_SIZE, cols = BOARD_SIZE):
    global environment
    environment = [[set() for _ in range(cols)] for _ in range(rows)]


#prevent out of bounds traversal
def validate_boundaries(new_position):
    if new_position[0] > BOARD_SIZE - 1 or new_position[0] < 0 or new_position[1] > BOARD_SIZE - 1 or new_position[1] < 0:
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

        print(f"{uid} moved from {previous} to {new_position}")

        request_redraw() #clear and redraw people to the GUI

        #if 2+ people occupy same position, log contact
        existing_residents = environment[new_position[0]][new_position[1]] # persons who already exist at the tile being moved to

        #there exists persons at the square moved to
        if len(existing_residents) > 1:
            contacted_map = { uid : [] } #keep track of the person being moved

            #loop through occupants and add to map ex. contacted_map = {'Bob' : {"Alice", "John"}} 
            for resident in existing_residents:
                if resident != uid:
                    contacted_map[uid].append(resident)
                    log_resident_contact(uid, resident) #log resident's perspective

            contact.append(contacted_map) #log contact for person who moved


            
#log contact from residents perspective
def log_resident_contact(uid, resident):
    contact.append({resident: [uid]})
    print(f"Contact between {uid} and {resident}")

# filter and return list for dictionaries that have the uid as a key
def trace_contact(uid):
    return [c for c in contact if uid in c]


######################### tkinter functionality #################################

#init the GUI
def start_gui():
    global root, canvas
    root = tk.Tk()
    root.title("Environment Tracker")
    canvas = tk.Canvas(root, width=BOARD_SIZE * CELL_SIZE, height=BOARD_SIZE * CELL_SIZE, bg="white")
    canvas.pack()
    draw_grid()
    root.mainloop()

#create the canvas matrix
def draw_grid():
    for i in range(BOARD_SIZE + 1):
        canvas.create_line(i * CELL_SIZE, 0, i * CELL_SIZE, BOARD_SIZE * CELL_SIZE)
        canvas.create_line(0, i * CELL_SIZE, BOARD_SIZE * CELL_SIZE, i * CELL_SIZE)

#clear and redraw all existing persons whenever position changes
def redraw():
    canvas.delete("person")  # remove only person markers, keep the grid lines
    for uid, (x, y) in positions.items():
        cx = x * CELL_SIZE + CELL_SIZE // 2
        cy = y * CELL_SIZE + CELL_SIZE // 2
        r = CELL_SIZE // 3
        canvas.create_oval(cx - r, cy - r, cx + r, cy + r, fill="skyblue", tags="person")
        canvas.create_text(cx, cy, text=uid, tags="person")

#because we have forever loops we must ensure we don't block
def request_redraw():
    if root is not None:
        root.after(0, redraw)

def start_consuming_thread():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()
    channel.queue_declare(queue='position', durable=True, arguments={'x-queue-type': 'quorum'})
    channel.queue_declare(queue='query', durable=True, arguments={'x-queue-type': 'quorum'})
    channel.queue_declare(queue='query_response', durable=True, arguments={'x-queue-type': 'quorum'})
    channel.basic_consume(queue='position', on_message_callback=position_callback, auto_ack=True)
    channel.basic_consume(queue='query', on_message_callback=query_callback, auto_ack=True)
    print('[Listening] Waiting for messages...')
    channel.start_consuming()

#############################################################################################


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
