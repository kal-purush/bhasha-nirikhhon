import numpy as np
import matplotlib.pyplot as plt


def interpolazioneDifferenzeDivise(x,f):
    diff_div=np.copy(f).astype(float)
    a=np.copy(f).astype(float)
    righe=np.shape(x)[0]
    for i in range(1,righe):
        diff_div=(diff_div[1:]-diff_div[0:-1])/(x[i:]-x[:-(i)])
        a[i]=diff_div[0]
    return a




def pol(coef, x):
    # Controlla che la lunghezza di coef e x sia coerente
    assert len(coef) == len(x), "Il numero di coefficienti deve essere uguale al numero di elementi in x."

    # Numero di coefficienti
    n = len(coef)

    # Inizializza la stringa della funzione polinomiale
    poly_string = f"{coef[0]}"

    # Aggiungi i termini del polinomio
    for i in range(1, n):
        poly_string += f" + {coef[i]}"
        for j in range(i):
            poly_string += f" * (t - {x[j]})"

    # Valuta la stringa come una funzione lambda
    polynomial = eval(f"lambda t: {poly_string}")
    return polynomial




x=np.asarray([1,2,4,5])
f=np.asarray([1,3,3,4])
a=interpolazioneDifferenzeDivise(x,f)
xx=np.linspace(0,5,100)
polynomial = pol(a, x)
yy = polynomial(xx)
plt.plot(xx,yy)
plt.plot(x,f,'o')
plt.show()