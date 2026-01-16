package ca.ucalgary.seng300.a1.test;
import org.lsmr.vending.*;
import org.lsmr.vending.hardware.*;
import ca.ucalgary.seng300.a1.*;

import java.util.ArrayList;

import org.junit.*;

public class TestVendingMachine {
	
	@Before
	public void Setup() {
		// Initialize default variables
		int[] coinTypes = {5, 10, 25, 100, 200};
		int numButtons = 6;
		int coinCap = 200;
		int popCap = 10;
		int reCap = 50;
		String[] names = {"Coke", "Sprite", "Root Beer", "default1", "default2", "default3"};
		int[] costs = {100, 75, 200, 50, 300, 25};
		
		// Initialize listeners
		CoinSlotListening slot = new CoinSlotListening();
		CoinRackListening[] racks = new CoinRackListening[coinTypes.length];
		SelectionButtonListening[] buttons = new SelectionButtonListening[numButtons];
		CoinReceptacleListening receptacle = new CoinReceptacleListening(reCap);
		PopCanRackListening[] canRacks = new PopCanRackListening[6];
		DeliveryChuteListening chute = new DeliveryChuteListening();
		
		// Turn arrays into array lists
		ArrayList<String> popNames = new ArrayList<String>(numButtons);
		for(String name:names) {
			popNames.add(name);
		}
		ArrayList<Integer> prices = new ArrayList<Integer>(numButtons);
		for(int cost:costs) {
			prices.add(cost);
		}
		
		// Setup vending machine
		VendingMachine machine = new VendingMachine(coinTypes, numButtons, coinCap, popCap, reCap);
		machine.configure(popNames, prices);
		machine.disableSafety();
		machine.getCoinSlot().register(slot);
		machine.getCoinReceptacle().register(receptacle);
		machine.getDeliveryChute().register(chute);
		for(int i = 0; i < coinTypes.length; i++) {
			racks[i] = new CoinRackListening(coinTypes[i]);
			machine.getCoinRack(i).register(racks[i]);
		}
		for(int i = 0; i < numButtons; i++) {
			buttons[i] = new SelectionButtonListening();
			machine.getSelectionButton(i).register(buttons[i]);
		}
		for(int i = 0; i < 6; i++) {
			canRacks[i] = new PopCanRackListening();
			machine.getPopCanRack(i).register(canRacks[i]);
		}
	}

}