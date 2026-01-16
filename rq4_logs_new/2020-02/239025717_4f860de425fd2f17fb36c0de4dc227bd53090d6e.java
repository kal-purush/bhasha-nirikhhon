package CoffeeShopSimulator;

import CoffeeShopSimulator.EventBus.Events.CoffeeShopEvent;
import CoffeeShopSimulator.EventBus.Events.CustomerLeavesEvent;
import CoffeeShopSimulator.EventBus.Events.NewCustomerWalksInEvent;
import CoffeeShopSimulator.EventBus.CoffeeShopEventBus;
import CoffeeShopSimulator.Models.Customer;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class CoffeeShop {
    private CoffeeShopEventBus coffeeShopEventBus;

    public CoffeeShop() {
        EventBus eventBus = new EventBus();
        coffeeShopEventBus = new CoffeeShopEventBus(eventBus);
        eventBus.register(this);
    }

    public void start() {
        Customer firstCustomer = new Customer("Rajeev");

        coffeeShopEventBus.sendEvent(new NewCustomerWalksInEvent(firstCustomer));
        coffeeShopEventBus.sendEvent(new CustomerLeavesEvent(firstCustomer));
    }

    @Subscribe
    public void handleNewCustomer(NewCustomerWalksInEvent e) {
        System.out.println(e.getCustomer().getName() + " arrived!");
        coffeeShopEventBus.addPersonToCoffeeShop(e.getCustomer());
    }

    @Subscribe
    public void handleCustomerLeaving(CustomerLeavesEvent e) {
        System.out.println(e.getCustomer().getName() + " left!");
        coffeeShopEventBus.removePersonFromCoffeeShop(e.getCustomer());
    }
}