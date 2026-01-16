package modelo;

public class Fecha{

	private int dia = 0, mes = 0, anio = 0; //Atributos: Caracteristicas de los objetos. Privado: Nadie lo puede modificar
	
	public Fecha(){} //Constructores, mismo nombre de las clases, construyen objetos. El primer constructor no recibe ni retorna valores.
	
	public Fecha(int dia, int mes, int anio){
	
		setDia(dia);
		setMes(mes);
		setAnio(anio);
		
	} //Constructor que recibe todos los datos. Lo que hace es llamar a los setters para ir asignando los valores que reciben al atributo correspondiente.
	
	public void setDia(int dia){
	
		this.dia = dia;
	} 
	//this porque estamos llamamdo igual al atributo que el parametro. This es del objeto actual del cual realizo la asignacion, en ese objeto en el atributo dia le asignamos el parametro que estamos recibiendo.
	
	public void setMes(int mes){ 
	
		this.mes = mes;
		
	}
	
	public void setAnio (int anio){
	
		this.anio = anio;
		
	}
	
	public int getDia(){
	
		return dia;
		
	}
	
	public int getMes(){
	
		return mes;
		
	}
	
	public int getAnio(){
	
		return anio;
		
	}
	
	public String toString(){
	
		return dia + "/" + mes + "/" + anio;
		
	}
	

}