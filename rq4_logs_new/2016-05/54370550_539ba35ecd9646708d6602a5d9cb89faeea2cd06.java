package visitor.structcode;

/**
 * Created by fisbii on 16-5-20.
 */
public class Client {
    public static void main(String args[]){
        ObjectStructure o = new ObjectStructure();
        o.attach(new ConcreteElementA());
        o.attach(new ConcreteElementB());

        ConcreteVisitor1 v1 = new ConcreteVisitor1();
        ConcreteVisitor2 v2 = new ConcreteVisitor2();

        o.accept(v1);
        o.accept(v2);
    }
}