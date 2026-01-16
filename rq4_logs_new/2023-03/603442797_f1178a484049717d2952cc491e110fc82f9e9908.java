package homework.day7.playground.processors;

import homework.day7.playground.essence.craft.Transportable;
import homework.day7.playground.utils.CoordinatesGenerator;

public class TransportableProcessor {

    public void runTransportable(Transportable transportable) {
        runTransportable(transportable, CoordinatesGenerator.generateCoordinate(), CoordinatesGenerator.generateCoordinate());
    }

    public void runTransportable(Transportable transportable, int pointA, int pointB) {
        int returnMove = transportable.move(pointA, pointB);
        System.out.printf("Transportable %s was moved to %s points", transportable.getClass().getSimpleName(), returnMove).println("");
    }
}