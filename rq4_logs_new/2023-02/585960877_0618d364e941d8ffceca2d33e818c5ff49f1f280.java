package ru.otus.bankomat;

import java.util.List;

public class Bankomat {
    private final MoneyStorageManager moneyStorageManager;

    private Bankomat() {
        this.moneyStorageManager = MoneyStorageManager.newInstance();
    }

    public static Bankomat newInstance() {
        return new Bankomat();
    }

    public void deposit(List<NominalType> banknotes) {
        moneyStorageManager.deposit(banknotes);
    }

    public long balance() {
        return moneyStorageManager.balance();
    }

    public void withdraw(int amount) {
        moneyStorageManager.withdraw(amount);
    }
}