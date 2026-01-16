import javax.xml.bind.JAXB;
import java.io.StringWriter;

public class Test {

    
    public static void main(String... args) {
        //System.out.println("hi ther efrom the java class");
        StringWriter sw = new StringWriter();
        
        JAXB.marshal(new Customer("robus", 45, "here"), sw);
        
        System.out.println(sw.toString());
    }
}

class Customer {
    
    public String name;
    public int age;
    public String hammer;

    public Customer(String name, int age, String hammer) {
        this.name = name;
        this.age = age;
        this.hammer = hammer;
    }

    
}