package m11_challenge1;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Challenge 5:
// Update the code so that the status variable is thread safe
// Rozwiązanie: lokalne zmienne są zawsze thread safe, więc nie trzeba nic zmieniać


public class BankAccountStatus {
    private double balance;
    private String accountNumber;
    private Lock lock;

    public BankAccountStatus(String accountNumber, double initialBalance) {
        this.accountNumber = accountNumber;
        this.balance = initialBalance;
        this.lock = new ReentrantLock();
    }

    public void deposit(double amount) {
        boolean status = false;
        try {
            if (lock.tryLock(1000, TimeUnit.MILLISECONDS)){
                try {
                    balance += amount;
                    status = true;
                } finally {
                    lock.unlock();
                }
            } else {
                System.out.println("Could not get the lock");
            }
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("Transaction status = " + status);
    }

    public void withdraw(double amount){
        boolean status = false;
        try {
            if (lock.tryLock(1000, TimeUnit.MILLISECONDS)){
                try {
                    balance -= amount;
                    status = true;
                } finally {
                    lock.unlock();
                }
            } else {
                System.out.println("Could not get the lock");
            }
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("Transaction status = " + status);
    }

    public double getBalance() {
        return balance;
    }

}