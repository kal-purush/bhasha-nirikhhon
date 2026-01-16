import matplotlib.pyplot as plt

def generate_pie_chart():
    labels = ['A', 'B', 'C']
    values = [30, 70 , 100]

    # Generando grafica de pastel
    fig, ax = plt.subplots()
    ax.pie(values, labels=labels) # Asignando valores y etiquetas (labels)
    plt.savefig('pie.png') # guardando grafica de pastel como imagen con nombre pie.png
    plt.close()