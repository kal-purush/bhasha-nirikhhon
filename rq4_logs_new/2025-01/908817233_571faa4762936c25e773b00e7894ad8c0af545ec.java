package Vending_Machine.vendingState;

import Vending_Machine.state;
import Vending_Machine.vendingMachine;

public class idelState implements state {

    @Override
    public void insertMoney(vendingMachine vm, double item) {

    }

    @Override
    public void selectItem(vendingMachine vm, double amount) {
        System.out.println("Please!! insert coin first.");
    }

    @Override
    public void dispenseItem(vendingMachine vm) {
        System.out.println("No transaction in progress.");
    }

    @Override
    public void refund(vendingMachine vm) {
        System.out.println("No balance to refund.");
    }
}