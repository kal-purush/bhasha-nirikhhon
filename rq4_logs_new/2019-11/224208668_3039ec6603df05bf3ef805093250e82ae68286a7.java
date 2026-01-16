package monopoly;

public class Casillas {
	public Casillas siguiente;//puntero enlace
	private int numeroCasilla;
	private String tipo;
	//private Jugador jugadorArriba;
	
	//Constructor para insertar al final
	public Casillas(int numero){
		this.numeroCasilla = numero;
		
	}
	//Constructor para insertar al incio
	public Casillas(int numero, Casillas n){
		this.numeroCasilla = numero;
		
		this.siguiente = n;
	}
	
	
	
	public void enlazarSiguiente(Casillas n){
		siguiente = n;
		
		
	}
	
	public Casillas obtenerSiguiente(){
		return siguiente;
	}



	public Object getNum() {
		return numeroCasilla;
	}

	public void setNum(int numero) {
		this.numeroCasilla = numero;
	}

	public String getTipo() {
		return tipo;
	}

	public void setTipo(String tipo) {
		this.tipo = tipo;
	}
	
	public void listarAtributos(){
		
	}
	
	
}