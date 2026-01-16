	
import time;
import serial;
import thread;
import sys;

arduino = serial.Serial('/dev/ttyUSB0', 9600, timeout=1.5);
rapidezActual = 0;
tiempoVariable = time.time();
tiempoInicio = time.time();

#variables de pid
if len(sys.argv) > 1 :
#	print(sys.argv[1]);
	velocidadReferencia = float(sys.argv[1]);
else :
	velocidadReferencia = 0.33;

errorOld = 0;
PIDvarOld = 0;
K = 2740.02;
ti = 1.1386;
Ts = 0.033;
#errorActual = 0;

proxSensor = 1024;
velToArduino = 0;
first = True;
rapidezAnterior = 0;
frenar = False;
frenado = -100;
ruido = 10;
frenadoEmergencia = False;
errorSensor = ruido;
errorSensor50 = 2;
totalStop = False;
val = -100;

LongitudRespo = 5
Longitud = 0;
def input_thread(L) :
	string = raw_input();
	L.append(string);

def map(x, inmin, inmax, outmin, outmax) :
	return (x-inmin)*(outmax-outmin)/(inmax-inmin)+outmin;

def PID(velocidad):
	global velocidadReferencia;
	global errorOld;
	global PIDvarOld;
	global K;
	global ti;
	global Ts;
#	global errorActual;

	errorActual = velocidadReferencia - velocidad;
	respuesta = PIDvarOld + (K/ti)*(Ts+ti)*errorActual - (K*errorOld);
#	print("Error: " + str(errorActual));
#	print("velocidad: " + str(velocidad));
#	print("Referencia: " + str(velocidadReferencia));
	
	errorOld = errorActual;
	PIDvarOld = respuesta;

	if respuesta > 100 :
		respuesta = 100;
	elif respuesta < -100:
		if frenar:
			respuesta = -100;
		else :
			respuesta = -10;

	return respuesta;

L = [];
thread.start_new_thread(input_thread, (L,));	

arduino.write("di50$");
rapidezFrenado = 0;

while True:
	#ver velocidad
	serialString = '';
	serialString = arduino.readline();
	if serialString == '':
		rapidezActual = 0;
	
	if time.time() - tiempoInicio > 0.3 and rapidezAnterior == rapidezActual and frenar == False : 
		rapidezActual -= 0.05;
		#PIDvarOld = 0;
		#errorOld = 0;
		
	#print(serialString);
	if "::prox" in serialString:
		proxSensor = int(serialString.split(' ')[1]);
#		print(int(proxSensor));
	
	if "::velup" in serialString and first == False or rapidezActual == 0 or first:
		if "::velup" in serialString :
			tiempoVariable = time.time() - tiempoInicio;
			#tiempoInicio = time.time();
			#4 cm radio = 12.5664   --- 1mm/ms = 1m/s
			#print("PID: " + str(velToArduino));
#			print("rapidez actual: " + str(rapidezActual));		
#			print("referencia: " + str(velocidadReferencia));
			#print("Error: " + str(errorActual));
			#print("tiempo var: " + str(tiempoVariable));
			rapidezAnterior = rapidezActual;
			rapidezActual = 125.664 / (tiempoVariable*1000);
			if rapidezActual > 5 :
				rapidezActual = rapidezAnterior;
			else :
				tiempoInicio = time.time();
				Longitud += 0.125664
	
		if Longitud<LongitudRespo and proxSensor > 30 and frenar == False:
			errorSensor = ruido;
			if rapidezActual < 0 and frenar:
				rapidezActual = 0.001;
			time.sleep(0.00001); 
			#errorSensor = ruido;
			frenado = -100;
			velToArduino = PID(rapidezActual);
			if velToArduino > 0:
				arduino.write("di" + str(velToArduino)+"$");
			elif velToArduino == 0:
				arduino.write("st$");
				#rapidezActual = 0;
			else :
				arduino.write("re"+str(velToArduino)+"$");
				if frenar == False:
					rapidezActual -= 0.05;
#				print("bajo rapidez");
			#print("PID: " + str(velToArduino) + "\n");
			print("rapidez, referencia, distancia, pid," + str(rapidezActual) +"," + str(velocidadReferencia) + "," + str(Longitud)  + "," + str(velToArduino) + ",");

#		if proxSensor < 100 and velocidadReferencia > 0.7: #and (rapidezActual < 0.7 or rapidezFrenado < 0.7) :
#			velocidadReferencia = 0.01;
#			rapidezFrenado = rapidezActual;
#			print("Frenado Emergencia");
#			arduino.write("re-100$");
#			time.sleep(0.1);
	else :
		first = False;

	if proxSensor < 60 :
		errorSensor50 -= 1;
		if errorSensor50 < 0:
			frenadoEmergencia = True;
	
#	print("ErrorSensor: " + str(errorSensor));
#	print("Tiempo: " + str(time.time() - Tiempo))
	if Longitud>LongitudRespo or proxSensor < 30:
		#arduino.write("st$");
		#print("detener");
		#velocidadReferencia = 0.00001;
		
		#velToArduino = 0;
		#proxSensor = serialString.split(' ')[1];
		#errorSensor =- 1;
		#if errorSensor < 0:
		#	frenar = True;

		freno = map(proxSensor, 17, 25, -60, 0);
		if freno < 0:
			#if rapidezActual > velocidadReferencia:
			#	totalStop = True;
			#else :
			arduino.write("re"+str(freno)+"$");
			
			print("Se encontro un obstaculo a:" + str(proxSensor)+"\n");
			#frenado += 9;
			totalStop = True;
			while val < 0 :
				arduino.write("re"+str(val)+"$");
				
#				if rapidezActual > 0.8 :
#					dec = 0.1;
#					val += 1;
#					time.sleep(float("0." + str(al)));
				if rapidezActual >= 0.5:
					dec = 1;
					val += 1;
				elif rapidezActual >= 0.3 and rapidezActual < 0.5 :
					dec = 5;
					val+= 5;
				elif rapidezActual >= 0.1 and rapidezActual < 0.3:
					dec = 10;
					val += 10;
			arduino.write("st$");
			print("freno: " + str(rapidezActual) + " | decremento: " + str(dec));
		#if proxSensor < 30:
		#	velToArduino = 5;
		#	pass
		#if proxSensor < 25:
		#	velToArduino = 50;
		#	pass

		#print("reversa en: " + str(velToArduino) + " \n ");
#	rapidezActual -= 0.01;

	#rapidexAnterior = rapidezActual;
	#manejo de thread para cambiar velocidad
	if L :
		try :
			velocidadReferencia = float(L[0]);
			print("La nueva velocidad de referencia sera: " + str(L[0]));
		except :
			print("no fue un numero valido");

		del L
		L = [];
		thread.start_new_thread(input_thread, (L,));
		pass