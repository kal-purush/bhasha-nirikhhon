package Homework5;

import javax.swing.plaf.IconUIResource;

public class Employees {
    private String name;
    private String position;
    private String email;
    private int phone;
    private int salary;
    private int age;

    public Employees (String name, String position, String email, int phone, int salary, int age ) {
        this.name = name;
        this.position = position;
        this.email = email;
        this.phone = phone;
        this.salary = salary;
        this.age = age;
    }

    public String getName(){
        return name;
    }
    public String getPosition () {
        return position;
    }
    public String getEmail () {
        return email;
    }
    public int getPhone () {
        return phone;
    }
    public int getSalary() {
        return salary;
    }
    public int getAge() {
        return age;
    }
    public void setEmail (String Email){
            if(Email.indexOf('@')==1){
                this.email=email;
        }
            else {
                System.out.println("Email is incorrect");
            }
    }
    public void setPhone (int phone){
        if (phone/1000==8){
            this.phone=phone;
        }else{
            System.out.println("Phone is incorrect");
        }
    }
    public void setSalary (int salary){
        if(salary>0){
            this.salary=salary;
        }else{
            System.out.println("Salary is negative");
        }
    }
    public void setAge (int age) {
        if (age > 0){
            this.age =age;
        }else{
            System.out.println("Age is incorrect");
        }
    }
    public void info (){
        System.out.println(this);
    }

    @Override
    public String toString() {
        return "Employees{" +
                "name='" + name + '\'' +
                ", position='" + position + '\'' +
                ", email='" + email + '\'' +
                ", phone=" + phone +
                ", salary=" + salary +
                ", age=" + age +
                '}';
    }
}