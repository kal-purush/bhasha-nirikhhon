package io.avalia.samba;

public class ViolaCaipira implements IInstrument {
	
	public String getColor() {
		return "yellow";
	}
	
	public int getSoundVolume() {
		return 10;
	}
	
	public String play(){
		return "plim plom";
	}
}