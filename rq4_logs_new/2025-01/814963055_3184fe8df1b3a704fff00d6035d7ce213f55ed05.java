package com.example.multithreadingProject.ATMSystem;

import com.example.multithreadingProject.ATMSystem.model.Atm;
import com.example.multithreadingProject.ATMSystem.model.Card;
import com.example.multithreadingProject.ATMSystem.model.TransactionType;
import com.example.multithreadingProject.ATMSystem.model.User;

public class ATMApplication {
    public static void main(String[] args) {
        Atm atm = new Atm();
        User user = new User("1", "John", 150.00);
        Card card = new Card("1", user, "1234", 1234);

        atm.insertCard(user, card);
        atm.authenticate(2345);
        atm.authenticate(1234);
        atm.selectTransactionType(TransactionType.WITHDRAW_CASH);
        atm.withdrawCash(160.00);
        atm.withdrawCash(140.00);
        atm.removeCard();
        System.out.println(user.getBalance());
    }
}