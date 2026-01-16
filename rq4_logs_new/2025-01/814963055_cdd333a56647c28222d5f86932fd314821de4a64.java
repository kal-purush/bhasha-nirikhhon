package com.example.multithreadingProject.VendingMachine;

import com.example.multithreadingProject.VendingMachine.model.InventoryManager;
import com.example.multithreadingProject.VendingMachine.model.Note;
import com.example.multithreadingProject.VendingMachine.model.Product;
import com.example.multithreadingProject.VendingMachine.model.VendingMachine;

import java.util.HashMap;
import java.util.Map;

public class VendingMachineApplication {
    public static void main(String[] args) {
        VendingMachine vendingMachine = new VendingMachine();
        vendingMachine.getInventoryManager().addProduct("Chocolate", new Product("Chocolate", 10.00, 2));
        vendingMachine.getInventoryManager().addProduct("Chips", new Product("Chips", 2.00, 1));

        vendingMachine.selectProduct("Chips");
        vendingMachine.insertNote(Note.FIVE);
        vendingMachine.dispenseProduct();
        vendingMachine.returnChange();
        vendingMachine.selectProduct("Chips");
        vendingMachine.insertNote(Note.TEN);
    }
}