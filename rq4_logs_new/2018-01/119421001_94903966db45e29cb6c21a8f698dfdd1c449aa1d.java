package de.fhro.inf.prg3.a10.kitchen;

import java.util.Deque;
import java.util.LinkedList;

import de.fhro.inf.prg3.a10.model.Dish;
import de.fhro.inf.prg3.a10.model.Order;

/**
 * Created by Lukas on 30.01.2018.
 */

public class KitchenHachtImpl implements KitchenHatch {

    private int maxMeals;
    private Deque<Order> orders;
    private Deque<Dish> dishes;

    public KitchenHachtImpl(int maxMeals, Deque<Order> orders){
        this.maxMeals = maxMeals;
        this.orders = orders;
        this.dishes = new LinkedList<>();
    }


    @Override
    public int getMaxDishes() {
        return maxMeals;
    }

    @Override
    public synchronized Order dequeueOrder(long timeout){
        Order dequeued = orders.pollFirst();
        if(dequeued == null){
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //try again after timeout period, maybe some orders are added in the meantime
            dequeued = orders.pollFirst();
        }
        return dequeued;
    }

    public int getOrderCount(){
        return orders.size();
    }

    public synchronized Dish dequeueDish(long timeout){
        Dish dequeued = dishes.pollFirst();
        if(dequeued == null){
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //try again after timeout period, maybe some dishes are added in the meantime
            dequeued = dishes.pollFirst();
        }
        return dequeued;
    }

    public synchronized void enqueueDish(Dish m){
        while(getDishesCount() == maxMeals){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        dishes.addLast(m);
    }

    @Override
    public int getDishesCount() {
        return dishes.size();
    }
}