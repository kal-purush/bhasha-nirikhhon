package ar.edu.unlam.basica2;

public class CajaDeAhorro extends CuentaSueldo {

	private Integer cantidadDeExtraccionesEfectuadas;
	public CajaDeAhorro() {
		super();
		cantidadDeExtraccionesEfectuadas=0;
	}
	@Override
	public void extraer(Double montoARetirar){
		if(montoARetirar<=(getSaldo()) ||
		(cantidadDeExtraccionesEfectuadas.intValue()>=5 && montoARetirar<=(getSaldo()+6))){
		setSaldo(getSaldo() - montoARetirar);
		
		cantidadDeExtraccionesEfectuadas++;
		
			if(cantidadDeExtraccionesEfectuadas.intValue()>=5){
				
					setSaldo(getSaldo()-6.0);
					
				}
		}
		return;
	}
}