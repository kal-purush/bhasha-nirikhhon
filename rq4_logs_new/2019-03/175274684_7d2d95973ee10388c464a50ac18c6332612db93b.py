def vel (xi, xf, t):                       #xi e xf são, respectivamente, a posição inicial e a final, em metros; e t é o tempo decorrido em segundos
    v = (xf-xi)/t
    print('Se a posição inicial do objeto for = ', xi, 'm')
    print('E sua posição final for = ', xf, 'm')
    print('E o tempo decorrido no percurso for = ', t, 's')
    print('A velocidade média é', v,  'm/s')
    print(' ')

vel(0, 10, 5)

def vel_ac (v0, t, a):                    #v0 é a velocidade inicial, em m/s; t é o tempo decorrido em segundos e a é a aceleração é m/(s*s)
    v = v0 + a*t
    print('Se o objeto estiver acelerado com a = ', a, 'm/(s*s):')
    print('Com velocidade inicial = ', v0, 'm/s')
    print('Durante um tempo transcorrido = ', t, 's')
    print('A velocidade média é', v,  'm/s')

vel_ac(2,5,10)
    
