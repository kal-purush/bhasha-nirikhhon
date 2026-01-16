import ast
import socketio
import time

io = socketio.Client()
io.connect('http://127.0.0.1:3000')

with open('./sample/t2.txt') as f:
    t = f.read()
with open('./sample/a2.txt') as f:
    a = f.read()

t = ast.literal_eval(t)
a = ast.literal_eval(a)
i=0
while i<len(t):
    io.emit('draw', {'torque': t[i:i+40], 'angle': a[i:i+40]})
    i+=40
    time.sleep(0.02)
