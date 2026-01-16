package com.evtimov.landlordapp.landlordspring.models;


import javax.persistence.*;

@Entity
@Table(name = "cards")
public class Card {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "CardId")
    private int cardID;

    @Column(name = "Brand")
    private String brand;

    @Column(name = "Type")
    private String type;

    @Column(name = "Cardnumber")
    private String cardNumber;

    @Column(name = "Cvvnumber")
    private int cvvNumber;

    @Column(name = "UserId")
    private int userID;

    @Column(name = "Balance")
    private double balance;

    @ManyToOne
    @JoinColumn(name = "UserId")
    private User user;

    public Card(){
        //default
    }

    public Card(int cardID, String brand, String type, String cardNumber, int cvvNumber, int userID, double balance, User user){
        setUser(user);
        setUserID(userID);
        setBrand(brand);
        setType(type);
        setCardNumber(cardNumber);
        setCvvNumber(cvvNumber);
        setCardID(cardID);
        setBalance(balance);
    }

    public int getCardID() {
        return cardID;
    }

    public void setCardID(int cardID) {
        this.cardID = cardID;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }

    public int getCvvNumber() {
        return cvvNumber;
    }

    public void setCvvNumber(int cvvNumber) {
        this.cvvNumber = cvvNumber;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}