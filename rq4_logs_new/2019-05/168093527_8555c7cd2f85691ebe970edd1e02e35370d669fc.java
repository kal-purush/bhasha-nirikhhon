package com.jvm.study.ch02.gengric;

public class NormalGeneric<T> {

    /**泛型类
     *  引入一个类型变量T（其他大写字母都可以，不过常用的就是T，E，K，V等等）
     */
    private T data;

    public NormalGeneric(){
    }

    public NormalGeneric(T data){
        this();
        this.data = data;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public static void main(String[] args) {
        NormalGeneric<String> stringNormalGeneric = new NormalGeneric<>();
        stringNormalGeneric.setData("peter is a ...");
        System.out.println(stringNormalGeneric.getData());
    }

}