package org.teng.jdk.reflect.basic;

/**
 * Created by Administrator on 6/14/15.
 */
public class CompileConstantTest {
    public static void main(String[] args){
        System.out.println(new ClsA());
    }
}

class Cls{
    static {
        System.out.println("static init area...");
    }

    public Cls(){
        System.out.println("new cls instantce..");
    }
    static  String compileConstant = "creazy java ppt";
}

class ClsA extends Cls{
    static {
        System.out.println("static init area in clsa...");
    }

    public ClsA(){
        System.out.println("new clsa instance...");
    }
    static String compileConstant = "creazy java ppt in clsa";
}