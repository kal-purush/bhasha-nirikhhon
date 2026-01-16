package flocking;

import mvc.Model;
import mvc.View;
import simstation.SimStationFactory;
import simstation.Simulation;

public class FlockingFactory extends SimStationFactory {

    @Override public Model makeModel() { return new FlockingSimulation(); }
    @Override   public String getTitle() { return "Flocking";}

    @Override public View makeView(Model m) {
        return new FlockingView((Simulation) m);
    }

}