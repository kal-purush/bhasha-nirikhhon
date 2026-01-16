package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson4.lection4;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Account implements Serializable {
    private double balance;
    private double maximumOverdraft;
    private String identifiantClient;
    private String code;
    private List<String> history;

    public Account(double balance, double maximumOverdraft, String identifiantClient, String code) {
        this.balance = balance;
        this.maximumOverdraft = maximumOverdraft;
        this.identifiantClient = identifiantClient;
        this.code = code;
        history = new ArrayList<>();
    }

    public String getIdentifiantClient() {
        return identifiantClient;
    }

    public String getCode() {
        return code;
    }

    public void debit(double somme) throws NegatifSumException {
        if(somme<0){
            throw new NegatifSumException("Somme can not be "+somme);
        }
        balance+=somme;
        history.add("Debit of "+somme+", "+ LocalDateTime.now());
    }

    public void credit(double somme) throws NegatifSumException, ImpossibleOperationException {
        if(somme<0){
            throw new NegatifSumException("Somme can not be "+somme);
        }
        if(somme > (balance+maximumOverdraft)) {
            throw new ImpossibleOperationException("You can not credit more than "+(balance+maximumOverdraft));
        }
        balance-=somme;
        history.add("Credit of "+somme+", "+ LocalDateTime.now());
    }

    public double getBalance() {
        return balance;
    }

    public double getMaximumOverdraft() {
        return maximumOverdraft;
    }

    public void checkBalance() {
        System.out.println("Your balance is "+balance+" $");
    }

    public String getHistory(){
        return history.stream().collect(Collectors.joining("\n"));
    }
}