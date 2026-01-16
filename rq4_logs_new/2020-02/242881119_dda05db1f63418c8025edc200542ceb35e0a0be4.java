package org.java.datastructure.stack;

public class TsDemo {
    /**
     * 基本数据类型的值传递,不改变其值
     * 引用数据类型的值传递,改变其值
     * <p>
     * String类虽然是引用数据类型,但是他当作参数传递时和基本数据类型是一样的
     */
    public static void main(String[] args) {
        String s = "abc";
        System.out.println(s);
        change(s);
        System.out.println(s);
        System.out.println("---------------------");

        System.out.println(s);
        changeString(s);
        System.out.println(s);


        System.out.println("---------------------");
        StringBuffer sb = new StringBuffer();
        sb.append("abc");
        System.out.println(sb);
        change(sb);
        System.out.println(sb);
    }

    public static void change(StringBuffer sb) {
        //调用该方法时实际参数的sb和形式参数的sb指向的是同一个对象(StringBuffer容器)
        //方法内部又在该容器里添加了"xyz",所以方法结束时,局部变量的sb消失,但是实际参数的sb所指向的容器的内部的内容已经发生了改变
        sb.append("xyz");
    }

    public static void change(String s) {
        s += "xyz";
    }

    //为了便于大家理解,再建立一个changeString方法
    public static void changeString(String str) {
        //因为str是属于局部变量,在调用该方法是实际参数s和形式参数str指向同一个字符串对象
        //但是在方法内部将str又指向了一个新的字符串对象,而此时s还是指向的原来的字符串对象
        //changeString方法执行完毕,局部变量str消失,方法内部产生的新的字符串对象称为垃圾
        //但是s还是指向的原有的字符串对象,并没有改变
        str += "xyz";
    }
}