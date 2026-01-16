public class Aguila extends Carnivoro{

	public Aguila(){
		super("agula", 200.5f, 500, "Conejo");
	}
	public Aguila(String nombre, float peso, String fuerza, String animalfavorito){
		super(nombre, peso, fuerza,  animalfavorito);
	}
	
	public void mostrarInfo(){
		System.out.println("Nombre: "+this.nombre);
		System.out.println("Peso: "+this.peso);
		System.out.println("presa favorita: "+this.animalfavorito);
		System.out.println("fuerza: "+this.fuerza);
	}
	
	
}