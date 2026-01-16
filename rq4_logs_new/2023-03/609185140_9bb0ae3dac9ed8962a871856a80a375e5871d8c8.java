package java_codes;

abstract class AbstractClass {
    int a = 10;
    int b;
    private int privateVar = 10;
    static int staticVar = 10;
    final static int finalVar = 10;

    public abstract void publicMethod();
//    private abstract void privateMethod(); // Can't create a private method
    protected abstract void defaultMethod();

    void methodWithBody(){
        System.out.println("Default implemented Method");
        System.out.println("Addition of privateVar + finalVar: "+(privateVar+finalVar));
    }
}