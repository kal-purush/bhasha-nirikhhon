package com.chengze;

public class Bicycle extends Bike {
        public int pedalNum;
        public Bicycle(){
            pedalNum = 2;
        }
        public Bicycle(int  pn){
            this.pedalNum= pn;
        }
    public String toString(){
        return String.valueOf(this.pedalNum);
    }

    public static void main(String[] args){
            Bike b= new Bike();
            Bike b1= new Bike(  2);
            System.out.println(b);
            System.out.println(b1);

            Bicycle Bicy= new Bicycle();
            Bicycle Bicy1= new Bicycle(2);
            System.out.println(Bicy);
            System.out.println(Bicy1);
            System.out.println("I am awesome!");
        }
}