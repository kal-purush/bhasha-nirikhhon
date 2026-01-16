//Realisation of interface IObserved. Has a list of all properties and choose the available one.
//Has a list of subscribers
import java.util.ArrayList;
import java.util.List;

public class AvailableForRent implements IObserved {
    ArrayList<Property>newListOfProperties= Main.newListOfProperties;

    ArrayList<Property> availableForRent = new ArrayList<>();

    List<IObserver> subscribers = new ArrayList<>();

    public void addAvailableProperty() {
       // ArrayList<Property>newListOfProperties= Main.newListOfProperties;
        System.out.println("List from Main "+ newListOfProperties);

        for (Property obj : newListOfProperties) {
            if (obj.hasLease() == false) {
                this.availableForRent.add(obj);
            }
            System.out.println("Available for rent "+ availableForRent);

            notifyObserver();
        }
    }

    public void removeAvailableProperty() {
      //  ArrayList<Property>newListOfProperties= Main.newListOfProperties;
        for (Property obj : newListOfProperties) {
            if (obj.hasLease() == true) {
                this.availableForRent.remove(obj);
            }
            notifyObserver();
        }
    }


    @Override
    public void addObserver(IObserver observer) {
    this.subscribers.add(observer);
    }

    @Override
    public void removeObserver(IObserver observer) {
        this.subscribers.remove(observer);
    }

    @Override
    public void notifyObserver() {
    for(IObserver observer: subscribers){
        observer.handleEvent(this.availableForRent);
    }
    }
}
