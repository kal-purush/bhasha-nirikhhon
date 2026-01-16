class Base
{
    Base()
    {
        jTest();
    }
    
    void jTest()
    {
        System.out.println("7");   
    }
}

public class Sub extends Base
{
    int jValue=17;

    Sub()
    {
        jTest();        
        System.out.println("This is Sub class");
        //jTest();
    }
    
    public static void main(String a[])
    {
        Sub sub = new Sub();/*this calls the Sub constructor which rather calls the Base cnostructor, Base constructor calls the jTest(), now since the object is of type Sub therefore this calls the jTest version of Sub class now, when it calls the jValue, that time the initilisation statement wasn't extecuted thus, it prints the default value that is zero,*/
/*Remember this is just a assumption made by my observations, the given reasons or the result may vary so please check it properly*/
        Base base = new Base();
        Sub subs = new Sub();
    }

    void jTest()
    {
        //jValue = 17;        
        System.out.println("This is jValue inside Sub class "+ jValue);
    }
}