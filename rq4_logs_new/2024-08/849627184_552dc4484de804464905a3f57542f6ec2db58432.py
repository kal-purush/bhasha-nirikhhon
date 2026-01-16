from flask import Flask, render_template, request, redirect, url_for
import sqlite3
from datetime import datetime, timedelta

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('precios.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('''CREATE TABLE IF NOT EXISTS precios
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     fecha TEXT NOT NULL,
                     producto TEXT NOT NULL,
                     precio_doto REAL NOT NULL,
                     precio_amazon REAL NOT NULL,
                     precio_mercadolibre REAL NOT NULL,
                     precio_walmart REAL NOT NULL)''')
    conn.commit()
    conn.close()

@app.route('/')
def index():
    conn = get_db_connection()
    precios = conn.execute('SELECT * FROM precios ORDER BY fecha DESC').fetchall()
    conn.close()
    return render_template('index.html', precios=precios)

@app.route('/agregar', methods=['GET', 'POST'])
def agregar_precio():
    if request.method == 'POST':
        fecha = datetime.now().strftime("%Y-%m-%d")
        producto = request.form['producto']
        precio_doto = float(request.form['precio_doto'])
        precio_amazon = float(request.form['precio_amazon'])
        precio_mercadolibre = float(request.form['precio_mercadolibre'])
        precio_walmart = float(request.form['precio_walmart'])

        conn = get_db_connection()
        conn.execute('INSERT INTO precios (fecha, producto, precio_doto, precio_amazon, precio_mercadolibre, precio_walmart) VALUES (?, ?, ?, ?, ?, ?)',
                     (fecha, producto, precio_doto, precio_amazon, precio_mercadolibre, precio_walmart))
        conn.commit()
        conn.close()
        return redirect(url_for('index'))

    return render_template('agregar.html')

@app.route('/analisis')
def analisis():
    fecha_inicio = request.args.get('fecha_inicio', '')
    fecha_fin = request.args.get('fecha_fin', '')
    marcas = request.args.getlist('marcas')

    conn = get_db_connection()
    query = 'SELECT * FROM precios WHERE 1=1'
    params = []

    if fecha_inicio and fecha_fin:
        query += ' AND fecha BETWEEN ? AND ?'
        params.extend([fecha_inicio, fecha_fin])

    query += ' ORDER BY fecha'
    precios = conn.execute(query, params).fetchall()
    conn.close()

    total_dias = len(precios)
    dias_mas_barato = 0
    for precio in precios:
        precios_competidores = []
        if 'Amazon' in marcas or not marcas:
            precios_competidores.append(precio['precio_amazon'])
        if 'MercadoLibre' in marcas or not marcas:
            precios_competidores.append(precio['precio_mercadolibre'])
        if 'Walmart' in marcas or not marcas:
            precios_competidores.append(precio['precio_walmart'])
        
        if precios_competidores and precio['precio_doto'] <= min(precios_competidores):
            dias_mas_barato += 1

    porcentaje_mas_barato = (dias_mas_barato / total_dias) * 100 if total_dias > 0 else 0

    fechas = [precio['fecha'] for precio in precios]
    precios_doto = [precio['precio_doto'] for precio in precios]
    precios_amazon = [precio['precio_amazon'] for precio in precios]
    precios_mercadolibre = [precio['precio_mercadolibre'] for precio in precios]
    precios_walmart = [precio['precio_walmart'] for precio in precios]

    return render_template('analisis.html', 
                           total_dias=total_dias, 
                           dias_mas_barato=dias_mas_barato, 
                           porcentaje_mas_barato=porcentaje_mas_barato,
                           fechas=fechas,
                           precios_doto=precios_doto,
                           precios_amazon=precios_amazon,
                           precios_mercadolibre=precios_mercadolibre,
                           precios_walmart=precios_walmart,
                           fecha_inicio=fecha_inicio,
                           fecha_fin=fecha_fin,
                           marcas_seleccionadas=marcas)

if __name__ == '__main__':
    init_db()
    app.run(debug=True)