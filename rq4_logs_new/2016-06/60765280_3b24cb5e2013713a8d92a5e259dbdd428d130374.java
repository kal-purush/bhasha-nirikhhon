package empleado;

public class empleado {
	//atributos
	private String nombreEmpleado;
	private String tipoEmpleado;
	private double ventasMes;
	private int horasExtra;
	//constructores
	public Empleado(){
	this.nombreEmpleado="desconocido";
	this.tipoEmpleado="VENDEDOR";
	this.ventasMes=0;
	this.horasExtra=0;
	}
	public Empleado(String nombreEmpleado, String tipoEmpleado, double ventasMes,
	int horasExtra){
	this.nombreEmpleado=nombreEmpleado;
	this.tipoEmpleado=tipoEmpleado;
	this.ventasMes=ventasMes;
	this.horasExtra=horasExtra;
	}
	public double calculaSalarioBruto(){
	int salarioBase, prima=0;
	double salarioBruto;
	if (tipoEmpleado.equals("VENDEDOR"))
		salarioBase=1000;
		else
		salarioBase=1500;
		if (ventasMes>=1000)
		prima=100;
		if (ventasMes>=1500)
		prima=200;
		salarioBruto=salarioBase+prima+(horasExtra*20);
		return salarioBruto;
		}
		public double calculaSalarioNeto(){
		double base, retencion=0, salario=0;
		base = calculaSalarioBruto();
		if (base<1000)
		retencion=0;
		else
		if (base>=1000 && base<=1500)
		retencion = 0.16;
		else
		retencion=0.18;
		salario = base * (1-retencion);
		return salario;
		}
		public String toString() {
		return "Empleado [nombreEmpleado=" + nombreEmpleado + ", tipoEmpleado="
		+ tipoEmpleado + ", ventasMes=" + ventasMes + ", horasExtra="
		+ horasExtra + "]";
		}
		}

