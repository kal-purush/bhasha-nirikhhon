package ar.edu.unlam.basica2.recursa3.tp2.cuentaBancaria;

public class CuentaBancaria {
	
	protected Double saldo;
	
	public CuentaBancaria(Double saldo){
		this.saldo = saldo;
	}
	
	public void depositarDinero(Double monto){
		this.saldo = this.saldo + monto;
	}
	
	public void extraerDinero(Double monto){
		this.saldo = this.saldo - monto;
	}

	
	
	
	
/////////////////////////// get y set /////////////////////////////
	
	
	public Double getSaldo() {
		return saldo;
	}

	public void setSaldo(Double saldo) {
		this.saldo = saldo;
	}

	
	
	
	
	
	
	
	
	
}