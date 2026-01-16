package com.example.fruitseason;

public class Model {
    String name, email, password, phone,address, city, province;
    public Model(String n, String e, String pass, String ph, String a, String c, String p){
        name = n;
        email = e;
        password= pass;
        phone = ph;
        address= a;
        city= c;
        province= p;

    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }


    public String getAddress() {
        return address;
    }

    public String getPassword() {
        return password;
    }

    public String getPhone() {
        return phone;
    }

    public String getCity() {
        return city;
    }

    public String getProvince() {
        return province;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setProvince(String province) {
        this.province = province;
    }
}