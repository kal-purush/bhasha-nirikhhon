package vehiculos;
public class Automovil extends Vehiculo{
	private int puestos;
	static int cantidadautomoviles;
	
	public Automovil(String placa, String nombre, int precio, int peso, Fabricante fabricante, int eje){
		super(placa,4, 100,nombre,precio,peso,"FWD", fabricante);
		this.puestos=puestos;
		cantidadautomoviles++;
	}
	int getPuestos() {
		return this.puestos;
	}
	
	void setPuestos(int puestos) {
		this.puestos=puestos;
	}
}