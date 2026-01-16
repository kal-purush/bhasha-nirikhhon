package com.fundamentos.fundamentos.bean;

public class MyBeanWithDependencyImplement implements MyBeanWithDependency {

    // Dependencia operacion
    private MyOperation myOperation;

    // Constructor
    public MyBeanWithDependencyImplement(MyOperation myOperation) {
        this.myOperation = myOperation;
    }

    @Override
    public void printWithDependency() {
        int numero = 1;
        System.out.println(myOperation.sum(numero));
        System.out.println("Hola desde la implementacion de Bean con dependencia");
    }
}