package org.woven.examples.factorymethod;

import org.woven.examples.exception.ToyTypeNotFoundException;
import org.woven.examples.model.Toy;
import org.woven.examples.model.ToyType;
import org.woven.examples.simplefactory.SimpleFactory;

public class ToysFactory {
    
    public SimpleFactory simpleFactory;
    
    public ToysFactory(SimpleFactory _simpleFactory) {
        this.simpleFactory = _simpleFactory;   
    }
    public Toy produceToy(ToyType toyType) throws ToyTypeNotFoundException {
        return simpleFactory.createToy(toyType);
    }

}