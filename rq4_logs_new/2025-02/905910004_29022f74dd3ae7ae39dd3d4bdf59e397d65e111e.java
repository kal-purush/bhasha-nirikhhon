package lesson_13_Extended_Synchronization.extra_task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Bar {
    private final List<Order> orders = new ArrayList<>();
    private final List<Drink> completeDrinks = new ArrayList<>();
    private final AtomicInteger bartendersAvailable = new AtomicInteger(2);

    public void putOrder(Client client, Menu chosenDrink) throws InterruptedException {
        synchronized (orders) {
            while (bartendersAvailable.get() == 0) {
                System.out.println("Client " + client.getClientID() + " is waiting for an available bartender...");
                orders.wait();
            }

            bartendersAvailable.decrementAndGet();
            System.out.println("Client " + client.getClientID() + " just ORDERED cocktail: "
                    + chosenDrink.name());
            client.addPrice(chosenDrink.getDrinkPrice());

            orders.add(new Order(client, chosenDrink));
            orders.notifyAll();
        }
    }

    public Order takeOrder() throws InterruptedException {
        synchronized (orders) {
            while (orders.isEmpty()) {
                orders.wait();
            }
            // Забираем первый заказ по очереди
            Order orderToPrepare = orders.remove(0);
            System.out.println("Bartender is taking an order for: "
                    + orderToPrepare.getChosenDrink().name());
            orders.notifyAll();
            return orderToPrepare;
        }
    }

    public Drink prepareDrink(Bartender bartender, Order order) throws InterruptedException {
        System.out.println("Bartender " + bartender.getBartenderID()
                + " started to PREPARE cocktail " + order.getChosenDrink().name());
        order.setStatus(Status.PREPARATION);
        Thread.sleep(3000); // Время приготовления

        Drink drink = new Drink(order);
        System.out.println("Bartender " + bartender.getBartenderID()
                + " FINISHED cocktail PREPARATION " + drink.getDrinkName()
                + " for client " + drink.getClientID());
        return drink;
    }

    public void provideDrink(Bartender bartender, Drink preparedDrink) throws InterruptedException {
        synchronized (completeDrinks) {
            preparedDrink.setDrinkStatus(Status.READY);
            completeDrinks.add(preparedDrink);
            System.out.println("Bartender " + bartender.getBartenderID()
                    + " released cocktail " + preparedDrink.getDrinkName()
                    + " for client " + preparedDrink.getClientID()
                    + " on the bar counter.");
            completeDrinks.notifyAll();
        }

        synchronized (orders) {
            bartendersAvailable.incrementAndGet();
            orders.notifyAll();
        }
    }

    public void takeDrink(Client client, Menu chosenDrink) throws InterruptedException {
        synchronized (completeDrinks) {
            while (true) {
                for (int i = 0; i < completeDrinks.size(); i++) {
                    Drink drinkInSight = completeDrinks.get(i);
                    if (drinkInSight.getDrinkName().equals(chosenDrink.name())
                            && drinkInSight.getDrinkStatus().equals(Status.READY)
                            && drinkInSight.getClientID() == client.getClientID()) {

                        completeDrinks.remove(drinkInSight);
                        System.out.println("Client " + client.getClientID()
                                + " is enjoying his " + drinkInSight.getDrinkName());
                        return;
                    }
                }
                completeDrinks.wait();
            }
        }
    }
}