from buildhat import Motor
from time import sleep
from datetime import datetime, timedelta
import requests

no2_motor = Motor('A') # configura motor deslizante
no2_motor.run_to_position (0,100) # restablece la posición del control deslizante
pm25_motor = Motor ('B') # configura el motor del medidor
pm25_motor.run_to_position (0,100) # restablece la posición del medidor

no2_min_valor = 0 # la lectura más baja de NO2 que crees que obtendrás (esperemos que sea alrededor de 0)
no2_max_valor = 60 #la lectura NO2 más alta que crees que obtendrás 
no2_min_angulo = 175 # recorrido mínimo del motor
no2_max_angulo = -175 # recorrido máximo del motor


pm25_min_valor = 0 # la lectura de NO2 más baja que crees que obtendrás (con suerte, debería ser alrededor de 0)
pm25_max_valor = 100 #la lectura de pm25 más alta que crees que obtendrás 
pm25_min_angulo = 175 # recorrido mínimo del motor
pm25_max_angulo = -175 # recorrido máximo del motor

base_url = "https://docs.openaq.org/v2/measurements"

payload = { #crea un diccionario para la solicitud API
    'date_from':'',
    'date_to':'',
    'location_id':'2480',
    'order_by':'datetime',
    'sort':'asc',
    'has_geo':'true',
    'limit':'100',
    'offset':'0',
}

contaminacion = {#crea un diccionario para las lecturas de contaminación
    'no2' : 0,
    'pm25': 0,
    }

def calidad_aire():
    ahora = datetime.now()
    delta = datetime.now() - timedelta(days=1)
    
    payload['date_from'] = f'{delta:%Y-%m-%d}T{delta:%H:%M:%S}+00:00'
    payload['date_to'] = f'{now:%Y-%m-%d}T{now:%H:%M:%S}+00:00'
    
    respuesta = requests.get(base_url, params=payload)
    
    if respuesta.status_code != 200:
        print ('no hay respuesta del servidor')
        return
    
    datos = respuesta.json ()
        
    for lectura in datos['results']:
        if lectura['parameter'] == 'no2': # Esto dependerá de qué contaminante estás midiendo
            contaminacion['no2'] = lectura['value']
        if lectura['parameter'] == 'pm25': # Esto dependerá de qué contaminante estás midiendo
            contaminacion['pm25'] = lectura['value']

    mostrar_resultado()   
    sleep(1)

def remap(min_valor, max_valor, min_angulo, max_angulo, sensor_datos):
    valor_rango = (max_valor - min_valor)        # averigua cuál es tu rango de valores
    motor_rango = (max_angulo - min_angulo) # averigua qué tan amplio es el rango de tu motor
    mapeado = (((sensor_datos - min_valor) * motor_rango) / valuor_rango) + min_angulo # estira tu rango de valores a lo largo del rango de tus motores
    return int(mapeado)        # devuelve un número que muestra el valor como un ángulo en el motor
   
def mostrar_resultado():
    print (f "NO2 = {contaminacion ['no2']}")
    no2_angulo_actual = no2_motor.get_aposition()
    no2_sensor_data = int(contaminacion ['no2'])
    no2_nuevo_angulo = remap (no2_min_valor, no2_max_valor, no2_min_angulo, no2_max_angulo, no2_sensor_datos)
    if no2_nuevo_angulo > no2_angulo_actual:
        no2_motor.run_to_position(no2_nuevo_angulo, 100, direction="clockwise")
    elif no2_nuevo_angulo < no2_angulo_actual:
        no2_motor.run_to_position(no2_nuevo_angulo, 100, direction="anticlockwise")
    sleep(0.1)
    pm25_sensor_datos = int(contaminacion['pm25'])
    print(f"PM2.5 = {contaminacion['pm25']}")
    pm25_angulo_actual = pm25_motor.get_aposition()
    pm25_nuevo_angulo = remap(pm25_min_valuor, pm25_max_valor, pm25_min_angulo, pm25_max_angulo, pm25_sensor_datos)
    pm25_motor.run_to_position(pm25_nuevo_angulo, 100)

while True:
    calidad_aire()
    sleep(3600)              # espera una hora antes de volver a preguntar (haz este número más pequeño para hacer pruebas)