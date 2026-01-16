package homework.day7.playground.runners;

import homework.day7.playground.essence.Flyable;
import homework.day7.playground.essence.craft.air.Aircraft;
import homework.day7.playground.essence.craft.air.Copter;
import homework.day7.playground.essence.craft.air.Plane;
import homework.day7.playground.essence.craft.air.Rocket;
import homework.day7.playground.essence.creatures.Fly;
import homework.day7.playground.essence.creatures.Insect;
import homework.day7.playground.essence.creatures.Mosquito;
import homework.day7.playground.processors.FlyableProcessor;

public class FlyableProcessorRunner {

    public static void main(String[] args) {
        FlyableProcessor Flyable_Processor = new FlyableProcessor();
        Flyable flyableCopter = new Copter(223, "Mi8 Flyable");
        Flyable flyablePlane = new Plane(3452, "Boeing 837 Flyable");
        Flyable flyableRocket = new Rocket(7623, "Super Heavy Flyable");
        Flyable flyableFly = new Fly(23, "Domestica Flyable");
        Flyable flyableMosquito = new Mosquito(12, "Vulgaris Flyable");
        Aircraft aircraftCopter = new Copter(223, "Mi8 Aircraft");
        Aircraft aircraftPlane = new Plane(3452, "Boeing 837 Aircraft");
        Aircraft aircraftRocket = new Rocket(7623, "Super Heavy Aircraft");
        Insect insectFly = new Fly(23, "Domestica Insect");
        Insect insectMosquito = new Mosquito(12, "Vulgaris Insect");
        Copter aCopter = new Copter(223, "Mi8 Copter");
        Plane aPlane = new Plane(3452, "Boeing 837 Plane");
        Rocket aRocket = new Rocket(7623, "Super Heavy Rocket");
        Fly aFly = new Fly(23, "Domestica Fly");
        Mosquito aMosquito = new Mosquito(12, "Vulgaris Mosquito");

        Flyable_Processor.runFlyable(flyableCopter);
        Flyable_Processor.runFlyable(flyablePlane);
        Flyable_Processor.runFlyable(flyableRocket);
        Flyable_Processor.runFlyable(flyableFly);
        Flyable_Processor.runFlyable(flyableMosquito);
        Flyable_Processor.runFlyable(aircraftCopter);
        Flyable_Processor.runFlyable(aircraftPlane);
        Flyable_Processor.runFlyable(aircraftRocket);
        Flyable_Processor.runFlyable((Flyable) insectFly);
        Flyable_Processor.runFlyable((Flyable) insectMosquito);
        Flyable_Processor.runFlyable(aCopter);
        Flyable_Processor.runFlyable(aPlane);
        Flyable_Processor.runFlyable(aRocket);
        Flyable_Processor.runFlyable(aFly);
        Flyable_Processor.runFlyable(aMosquito);

        System.out.println();
        Flyable_Processor.runFlyable(flyableCopter, "никуда");
        Flyable_Processor.runFlyable(flyablePlane, "повсюду");
    }


}