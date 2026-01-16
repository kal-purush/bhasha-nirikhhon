package org.eclipse.che.examples;

import org.eclipse.che.examples.exception.ToyTypeNotFoundException;
import org.eclipse.che.examples.model.Car;
import org.eclipse.che.examples.model.NullToy;
import org.eclipse.che.examples.model.Scooter;
import org.eclipse.che.examples.model.Toy;
import org.eclipse.che.examples.model.ToyType;
import org.eclipse.che.examples.model.Truck;

public class ToysFactory {
    
    public Toy produceToy(ToyType toyType) throws ToyTypeNotFoundException {
        Toy toy;
        switch(toyType) {
            case CAR : toy = new Car(4);break;
            case TRUCK : toy = new Truck(6);break;
            case SCOOTER : toy = new Scooter(2);break;
            default : toy = new NullToy();
        }

        return toy;
    }

}