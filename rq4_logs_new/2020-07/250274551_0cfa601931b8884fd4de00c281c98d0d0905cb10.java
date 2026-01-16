package com.zubko.lectures.lecture_28;

public class User {
    private String name;
    private String surname;
    private Address address;

    public User(String name, String surname, Address address) {
        this.name = name;
        this.surname = surname;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public Address getAddress() {
        return address;
    }

//    public Address getAdress() {
//        return address;
//    }
//
//    public void setAdress(Address adress) {
//        this.address = adress;
//    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", address=" + address +
                '}';
    }
}