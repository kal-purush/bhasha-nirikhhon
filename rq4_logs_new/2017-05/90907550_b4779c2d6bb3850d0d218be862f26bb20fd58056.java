package ar.edu.unlam.basica2.recursa3.tp2.cuentaBancaria;

public class CajaDeAhorro extends CuentaBancaria {

	

	final private Double RECARGO = 6.00;
	private String saldoErroneo;
	private Integer contadorDeExtracciones;
	
	
	public CajaDeAhorro(Double saldo){
		super(saldo);
		this.contadorDeExtracciones = 0;
	
	}

	@Override
	public void extraerDinero(Double monto){
		
		if(this.saldo >=  monto){
			this.saldo = this.saldo - monto;
			this.contadorDeExtracciones ++;
		}else{
			this.saldoErroneo = "Error: Saldo insuficiente";
		}
	}

	public void contador(){
		if(this.contadorDeExtracciones.equals(5)){
			this.saldo = this.saldo - RECARGO;
			this.contadorDeExtracciones = 0;
		}
	}

	
}