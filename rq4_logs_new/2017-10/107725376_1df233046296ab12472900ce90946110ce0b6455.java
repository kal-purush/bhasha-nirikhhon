import org.lsmr.vending.*;
import org.lsmr.vending.hardware.*;
import ca.ucalgary.seng300.a1.*;
import java.util.*;

public class Test {
	public static void main(String[] args) throws DisabledException {
		Scanner keyboard = new Scanner(System.in);
		String input = "";
		int[] coinTypes = {5, 10, 25, 100, 200};
		int numButtons = 6;
		int coinCap = 200;
		int popCap = 10;
		int reCap = 50;
		CoinSlotListening slot = new CoinSlotListening();
		CoinRackListening[] racks = new CoinRackListening[coinTypes.length];
		ArrayList<String> popNames = new ArrayList<String>(6);
		String[] names = {"Coke", "Sprite", "Root Beer", "default1", "default2", "default3"};
		for(String name:names) {
			popNames.add(name);
		}
		ArrayList<Integer> prices = new ArrayList<Integer>(6);
		int[] costs = {100, 75, 200, 50, 300, 25};
		for(int cost:costs) {
			prices.add(cost);
		}
		SelectionButtonListening[] buttons = new SelectionButtonListening[numButtons];
		CoinReceptacleListening receptacle = new CoinReceptacleListening(reCap);
		PopCanRackListening[] canRacks = new PopCanRackListening[6];
		
		VendingMachine machine = new VendingMachine(coinTypes, numButtons, coinCap, popCap, reCap);
		machine.configure(popNames, prices);
		machine.disableSafety();
		machine.getCoinSlot().register(slot);
		machine.getCoinReceptacle().register(receptacle);
		for(int i = 0; i < coinTypes.length; i++) {
			racks[i] = new CoinRackListening(coinTypes[i]);
			machine.getCoinRack(i).register(racks[i]);
		}
		for(int i = 0; i < numButtons; i++) {
			buttons[i] = new SelectionButtonListening();
			machine.getSelectionButton(i).register(buttons[i]);
		}
		
		while(!input.equalsIgnoreCase("quit")) {
			//interpret inputs
			Coin insert = null;
			input = keyboard.next();
			switch(input) {
			case "b1": {
				machine.getSelectionButton(0).press();
				break;
			}
			case "b2": {
				machine.getSelectionButton(1).press();
				break;
			}
			case "b3": {
				machine.getSelectionButton(2).press();
				break;
			}
			case "b4": {
				machine.getSelectionButton(3).press();
				break;
			}
			case "b5": {
				machine.getSelectionButton(4).press();
				break;
			}
			case "b6": {
				machine.getSelectionButton(5).press();
				break;
			}
			default: {
				try {
					int i = Integer.parseInt(input);
					insert = new Coin(i);
				}
				catch(NumberFormatException e) {}
			}
			}
			if(insert != null) {
				machine.getCoinSlot().addCoin(insert);
			}
			for(int i = 0; i < numButtons; i++) {
				if(buttons[i].isPressed()) {
					if(!canRacks[i].isEmpty()) {
						if(receptacle.getValue() >= costs[i]) {
							//TODO subtract cost and dispense pop
						}
					} else {
						//out of pop in the rack
					}
				}
			}
		}
		keyboard.close();
	}
	
}