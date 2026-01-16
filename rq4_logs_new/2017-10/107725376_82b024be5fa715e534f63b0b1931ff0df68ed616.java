//SENG300 Group Assignment 1
//Tae Chyung, Cameron Davies & Grace Ferguson

package ca.ucalgary.seng300.a1.test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import org.lsmr.vending.*;
import org.lsmr.vending.hardware.*;

import ca.ucalgary.seng300.a1.*;

public class VendingMachineTest {

	private VendingMachine machine;
	private PopCanRackListening[] canRacks;
	private SelectionButtonListening[] buttons;
	private CoinReceptacleListening receptacle;
	private DeliveryChuteListening chute;

	/**
	 * setup to initialize vending machine and accompanying listeners
	 */
	@Before
	public void setup() {
		int[] coinTypes = { 5, 10, 25, 100, 200 };
		int numButtons = 6;
		int coinCap = 200;
		int popCap = 10;
		int reCap = 50;
		CoinSlotListening slot = new CoinSlotListening();
		CoinRackListening[] racks = new CoinRackListening[coinTypes.length];
		ArrayList<String> popNames = new ArrayList<String>(6);
		String[] names = { "Pop1", "Pop2", "Pop3", "Pop4", "Pop5", "Pop6" };
		for (String name : names) {
			popNames.add(name);
		}
		ArrayList<Integer> prices = new ArrayList<Integer>(6);
		int[] costs = { 250, 250, 250, 250, 250, 250 };
		for (int cost : costs) {
			prices.add(cost);
		}

		machine = new VendingMachine(coinTypes, numButtons, coinCap, popCap, reCap);

		// communicator needs to be created before selection buttons, since
		// selection button takes in a reference to the communicator
		VendCommunicator communicator = new VendCommunicator();

		buttons = new SelectionButtonListening[numButtons];
		receptacle = new CoinReceptacleListening(reCap);
		canRacks = new PopCanRackListening[6];
		chute = new DeliveryChuteListening();

		machine.configure(popNames, prices);
		machine.disableSafety();
		machine.getCoinSlot().register(slot);
		machine.getCoinReceptacle().register(receptacle);
		machine.getDeliveryChute().register(chute);
		for (int i = 0; i < coinTypes.length; i++) {
			racks[i] = new CoinRackListening(coinTypes[i]);
			machine.getCoinRack(i).register(racks[i]);
		}
		for (int i = 0; i < numButtons; i++) {
			buttons[i] = new SelectionButtonListening(i, communicator);
			machine.getSelectionButton(i).register(buttons[i]);
		}
		for (int i = 0; i < 6; i++) {
			canRacks[i] = new PopCanRackListening();
			machine.getPopCanRack(i).register(canRacks[i]);
			machine.getPopCanRack(i).load(new PopCan(machine.getPopKindName(i)));
		}

		communicator.linkVending(receptacle, canRacks, machine);

	}

	/**
	 * tests regular input into machine. Expected result: success, can rack is
	 * empty
	 * 
	 * @throws DisabledException
	 */
	@Test
	public void regularUseTest() throws DisabledException {
		assertFalse(canRacks[3].isEmpty());
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getSelectionButton(3).press();
		assertTrue(canRacks[3].isEmpty());
	}

	/**
	 * Tests an invalid coin input. Expected result: pop has not been dispensed
	 * 
	 * @throws DisabledException
	 */
	@Test
	public void invalidCoinTest() throws DisabledException {
		machine.getCoinSlot().addCoin(new Coin(300));
		machine.getSelectionButton(1).press();
		assertFalse(canRacks[1].isEmpty());
	}

	/**
	 * Insufficient funds are input into machine. Expected result: pop has not
	 * been dispensed
	 * 
	 * @throws DisabledException
	 */
	@Test
	public void insufficientFundsTest() throws DisabledException {
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getSelectionButton(2).press();
		assertFalse(canRacks[2].isEmpty());
	}

	/**
	 * rack is loaded with two pops, both are dispensed. Expected result: rack
	 * is empty
	 * 
	 * @throws DisabledException
	 */
	@Test
	public void multiplePopTest() throws DisabledException {
		machine.getPopCanRack(2).load(new PopCan(machine.getPopKindName(2)));
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getSelectionButton(2).press();
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getSelectionButton(2).press();
		assertTrue(canRacks[2].isEmpty());
	}

	/**
	 * rack is loaded with three pops, two are dispensed. Expected result: rack
	 * is not empty
	 * 
	 * @throws DisabledException
	 */
	@Test
	public void multiplePopTest2() throws DisabledException {
		machine.getPopCanRack(2).load(new PopCan(machine.getPopKindName(2)));
		machine.getPopCanRack(2).load(new PopCan(machine.getPopKindName(2)));
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getSelectionButton(2).press();
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getSelectionButton(2).press();
		assertFalse(canRacks[2].isEmpty());
	}

	/**
	 * dispense pop so that rack is empty and then try to purchase. Expected
	 * result: rack is empty, pop is not dispensed
	 * 
	 * @throws DisabledException
	 */
	@Test
	public void emptyRackTest() throws DisabledException {
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getSelectionButton(3).press();
		assertTrue(canRacks[3].isEmpty());

		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));

		machine.getSelectionButton(3).press();

	}

	/**
	 * rack holds two pops, one pop is purchased, insufficient funds are added
	 * for second pop. Expected result: one pop is dispensed, other pop remains
	 * in rack
	 * 
	 * @throws DisabledException
	 */
	@Test
	public void multiplePopInsufficientFundTest() throws DisabledException {
		machine.getPopCanRack(4).load(new PopCan(machine.getPopKindName(4)));
		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getCoinSlot().addCoin(new Coin(25));
		machine.getSelectionButton(4).press();

		machine.getCoinSlot().addCoin(new Coin(200));
		machine.getSelectionButton(4).press();
		assertFalse(canRacks[4].isEmpty());
	}

}