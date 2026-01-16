import com.sun.org.apache.xerces.internal.dom.DOMErrorImpl;

public class TempTest {

    int oldField = 10;

    TempTest(int o) {
        oldField = o;
    }

    public static void main(String[] args) {
        TempTest test = new TempTest(9);
        Derived derived = Derived.getDerived(test, 15);
        System.out.println(derived.newField);
    }
}

class Derived extends TempTest{

    int newField = 20;

    Derived(int o, int n) {
        super(o);
        this.newField = 19;
    }

    static Derived getDerived(TempTest temp, int ne) {
        Derived derived = (Derived) temp;
        derived.newField = ne;
        return  derived;
    }
}