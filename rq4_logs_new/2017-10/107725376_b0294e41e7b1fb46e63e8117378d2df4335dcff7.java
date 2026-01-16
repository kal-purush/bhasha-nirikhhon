package ca.ucalgary.seng300.a1;
import org.lsmr.vending.*;
import org.lsmr.vending.hardware.*;

public class DeliveryChuteListening implements DeliveryChuteListener {

	boolean active;
	Deliverable[] itemsReturned;
	@Override
	public void enabled(AbstractHardware<? extends AbstractHardwareListener> hardware) {
		active = true;
	}

	@Override
	public void disabled(AbstractHardware<? extends AbstractHardwareListener> hardware) {
		active = false;
	}

	// When an item is sent to the delivery chute, empties the chute, and checks if the item was a popcan and outputs it
	// needs to be tested
	public void itemDelivered(DeliveryChute chute) {
		itemsReturned = chute.removeItems();
		for(int i = 0; i < itemsReturned.length; i++)
		{
			if(itemsReturned[i] instanceof PopCan)
			{
				System.out.println(itemsReturned[i].toString() + " has been dispensed");
			}
		}
	}

	@Override
	public void doorOpened(DeliveryChute chute) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doorClosed(DeliveryChute chute) {
		// TODO Auto-generated method stub

	}

	@Override
	public void chuteFull(DeliveryChute chute) {
		// TODO Auto-generated method stub

	}

}