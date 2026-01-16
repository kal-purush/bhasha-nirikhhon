package taller3.televisores;

public class Control {
	
	private TV tv;

	public void setTv (TV tv) {
		
		this.tv = tv;
	}
	
	public TV getTv() {
		
		return this.tv;
	}
	
	public void turnOn (){
		
		tv.estado = true;
				
	}
	
	public void turnOff () {
		
		tv.estado = false;
	}
	
	public void canalUp(){
		
		tv.canalUp();
		
	}
	
	public void canalDown() {
			
		tv.canalDown();
					
	}
	
	public void volumenUp () {
		
		tv.volumenUp();
		
	}
	
	public void volumenDown() {
		
		tv.volumenDown();
}
	
	public void setVolumen (int volumen) {
		
		if ((tv.estado && volumen>= 0) && (volumen <= 7)) {
			tv.volumen = volumen;
		}
		
	}
	
	public void setCanal (int canal) {
		
		if ((tv.estado && canal <= 120) && ( canal>=1)) {
				tv.canal = canal;
		}
		
	}
	
	public void enlazar(TV tv) {
		
		this.tv = tv;
		tv.setControl(this);
	}
	
}