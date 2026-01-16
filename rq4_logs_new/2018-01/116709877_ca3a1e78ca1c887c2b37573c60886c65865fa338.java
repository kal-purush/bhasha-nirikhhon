package pl.kostrowski.nauka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private final static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        TestClass a = new TestClass();
        TestClass b = new TestClass();
        a.value = "Biały";
        b.value = "Czarny";

        LOG.info("Przed zmianą w main");
        LOG.info("A " + a.value);
        LOG.info("B " + b.value);

        Swapper swapper = new Swapper();

        swapper.swap(a, b);

        LOG.info("Po zmianie w main");
        LOG.info("A " + a.value);
        LOG.info("B " + b.value);

    }


}

class Swapper {
    private final static Logger LOG = LoggerFactory.getLogger(Swapper.class);

    public void swap(TestClass a, TestClass b) {
        TestClass temp;

        LOG.info("Przed zmianą wewnątrz swappera");
        LOG.info("A " + a.value);
        LOG.info("B " + b.value);

        temp = a;
        a = b;
        b = temp;

        LOG.info("po zmianie wewnątrz swappera");
        LOG.info("A " + a.value);
        LOG.info("B " + b.value);
    }

}

