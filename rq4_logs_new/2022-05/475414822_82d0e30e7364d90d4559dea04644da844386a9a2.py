from multiprocessing import Process, Queue, Pipe
import os, sys


def rot13(value):
    Rot13=''
    abc = 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ'
    for i in value:
        if i in abc:
            Rot13 += abc[abc.index(i) + 13]
        else:
            Rot13 += i
    return Rot13
        


def escritor(q,pipe_w):
    print("Escritor escribiendo %d" % os.getpid())
    sys.stdin = open(0)
    print("Ingrese una linea: ")
    c = sys.stdin.readline()
    pipe_w.send(c)
    while q:
        try:
            print("Lector leyendo: %s" % q.get(True, 1))
        except:
            print("Cola vacia... saliendo")
            break
    


def lector(q,pipe_r):
    valor = pipe_r.recv()
    if valor:
        encrypted = rot13(valor)
        q.put(encrypted)
        pipe_r.send("Ya lei...")



def main():
    r,w = Pipe()
    q = Queue()

    p1 = Process(target=escritor, args=(q,w))
    p2 = Process(target=lector, args=(q,r))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    r.close()
    w.close()
    print("Padre terminando...")


if __name__ == "__main__":
    main()    