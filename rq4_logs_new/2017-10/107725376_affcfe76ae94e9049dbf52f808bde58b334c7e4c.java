package ca.ucalgary.seng300.a1;
import org.lsmr.vending.*;
import org.lsmr.vending.hardware.*;

public class CoinRackListening implements CoinRackListener {
	private boolean isOn;
	private int coinCount;
	private int value;
	
	public CoinRackListening(int value) {
		this.value = value;
	}

	public void enabled(AbstractHardware<? extends AbstractHardwareListener> hardware) {
		isOn = true;
	}

	public void disabled(AbstractHardware<? extends AbstractHardwareListener> hardware) {
		isOn = false;
	}
	
	public void coinsFull(CoinRack rack) {}

	public void coinsEmpty(CoinRack rack) {}

	public void coinAdded(CoinRack rack, Coin coin) {
		coinCount++;
	}

	public void coinRemoved(CoinRack rack, Coin coin) {
		coinCount = 0;
	}

	public void coinsLoaded(CoinRack rack, Coin... coins) {
		coinCount += coins.length;
	}

	public void coinsUnloaded(CoinRack rack, Coin... coins) {
		coinCount -= coins.length;
	}
	
	public int getValue() {
		return value;
	}
	
	public boolean isOn() {
		return isOn;
	}
	
	public int getCoins() {
		return coinCount;
	}

}