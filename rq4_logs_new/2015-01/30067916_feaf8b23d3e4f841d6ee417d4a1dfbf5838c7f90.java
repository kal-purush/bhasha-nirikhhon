import lejos.nxt.*;



public class BarcodeReader implements SensorPortListener {
	private DataExchange DE;
	
	private int i = 0;
	
	public BarcodeReader(DataExchange d) {
		DE = d;
	}

	public void stateChanged(SensorPort aSource, int aOldValue, int aNewValue) {
		
		if (!DE.getDoBarcodeScan()) {return;}
		
		LCD.clear();
		LCD.drawInt(Math.abs(aNewValue-aOldValue), 0, 0);
		LCD.drawInt(DATA.ls.getLightValue(), 0, 1);
		LCD.drawInt(i, 0, 2);
		
		if (Math.abs(aNewValue-aOldValue)<DATA.barcodeTres) { return; }
		
		i++;
	//	LCD.drawInt(aOldValue, 0, 1);
		
	//	LCD.drawString("SWITCH", 0,1);
		if (i > DATA.switchesTres) { DE.setBarcodeDetected(true); DE.setChallengeEnded(true);}
		
	}

}