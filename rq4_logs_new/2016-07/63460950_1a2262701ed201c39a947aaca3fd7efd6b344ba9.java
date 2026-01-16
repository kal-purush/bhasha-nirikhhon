import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class PicoyPlacaPredict {
	
	public static void main(String []args) {
	
		//Programa que predice si un auto puede o no circular en determinado dia y hora en Quito
		//Se inicializan las variables
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String placa="", hora="", fecha ="";
		
		System.out.println("Bienvenido al programa Pico y Placa"); // Comienzo del programa		
		//Ingreso de los valores por el usuario
		System.out.println("Ingresar el n�mero de placa: ");
		try {
			placa = br.readLine();
		System.out.println("Ingresar la hora en formato de 24 horas ejem: 14:30: ");
	       hora = br.readLine();
		System.out.println("Ingresar la fecha en el siguiente formato dia/mes/a�o ejem: 15/07/2016: ");
			fecha = br.readLine();			 
		} catch (IOException e) {
			e.printStackTrace();;}
			
			try {
				getInfo(placa, hora, fecha);
			} catch (ParseException e) {
				System.out.print("Formato incorrecto ingresar de nuevo los datos");;
			}			   				
			}
	
	public static String getInfo(String placa, String hora, String fecha) throws ParseException{
		
		//Convierte la fecha string  a date
		DateFormat format = new SimpleDateFormat("dd/MM/yyyy");
		 Date date = format.parse(fecha);
		 
		 String SacaAuto = "Puede sacar el auto en este d�a y hora";
		 String NoSacaAuto = "No puede sacar el auto en este dia y hora";
					
		//Obtiene el ultimo digito de la placa//
		 char lastDigit = placa.charAt(placa.length()-1);
		 /////////////////////////////////////////////////
		 //Calcula el dia de la semana seg�n la fecha;
		 GregorianCalendar cal = new GregorianCalendar();
         cal.setTime(date);
         int day = cal.get(Calendar.DAY_OF_WEEK);
		        
         //Convert  string to time//        
         SimpleDateFormat formato = new SimpleDateFormat("HH:mm");
         Date time = (Date)formato.parse(hora);
         Date dMin = formato.parse("07:00");
         Date dMax = formato.parse("09:30");
         Date dMinT = formato.parse("16:00");
         Date dMaxT = formato.parse("19:30");
         
           
         ///Realiza las validaciones para calcular si puede o no circular//
        switch (day) {
            case 1:  if (time.getTime()>=dMin.getTime() && time.getTime()<=dMax.getTime() 
            		|| lastDigit ==1 || lastDigit ==2 
            		||time.getTime()>=dMinT.getTime() && time.getTime()<=dMaxT.getTime()   ){
            	
            	System.out.println(NoSacaAuto);
            }else
            {
            	System.out.println(SacaAuto);
            	
            }
                     break;
                     
            case 2:  if (time.getTime()>=dMin.getTime() && time.getTime()<=dMax.getTime() 
            		|| lastDigit ==3 || lastDigit ==4 
            		||time.getTime()>=dMinT.getTime() && time.getTime()<=dMaxT.getTime()){
            	
            	System.out.println(NoSacaAuto);;
            }else
            {
            	System.out.println(SacaAuto);
            }
                     break;
            case 3: if (time.getTime()>=dMin.getTime() && time.getTime()<=dMax.getTime() 
            		|| lastDigit ==5 || lastDigit ==6 
            		||time.getTime()>=dMinT.getTime() && time.getTime()<=dMaxT.getTime()){
            	
            	System.out.println(NoSacaAuto);
            }else
            {
            	System.out.println(SacaAuto);;
            }
                     break;
            case 4:  if (time.getTime()>=dMin.getTime() && time.getTime()<=dMax.getTime() 
            		|| lastDigit ==7 || lastDigit ==8 
            		||time.getTime()>=dMinT.getTime() && time.getTime()<=dMaxT.getTime()){
            	
            	System.out.println(NoSacaAuto);
            }else
            {
            	System.out.println(SacaAuto);
            }
                     break;
            case 5: if (time.getTime()>=dMin.getTime() || time.getTime()<=dMax.getTime() 
            		|| lastDigit ==9 || lastDigit ==0 
            		||time.getTime()>=dMinT.getTime() || time.getTime()<=dMaxT.getTime()){
            	
            	System.out.println(NoSacaAuto);
            }else
            {
            	System.out.println(SacaAuto);
            }
                     break;
            case 6:
            
            	System.out.println(SacaAuto);
            
                     break;
            case 7:  
            	System.out.println(SacaAuto);
          
                     break;
        }
					     	
		return placa;
				
}}







