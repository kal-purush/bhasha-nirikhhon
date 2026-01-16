package ShopExampleProcedurally;

import ShopExampleProcedurally.Data.*;

public class Main {
    public static void main(String[] args) {
        Boss manager = new Boss("Miggy");
        Employee barista = new Employee("Rajeev");
        Shop coffeeShop = new Shop(manager, barista);

        Guest aGuest = new Guest("Emily");
        coffeeShop.newCustomerWalksIn(aGuest);
        coffeeShop.customerOrders(aGuest);
        coffeeShop.employeeMakesAllOrders();
        coffeeShop.customerPickUp(aGuest);
        coffeeShop.customerLeaves(aGuest);


    }
}