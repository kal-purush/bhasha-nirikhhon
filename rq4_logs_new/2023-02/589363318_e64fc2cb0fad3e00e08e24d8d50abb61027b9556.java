package frc.robot.implementation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import frc.robot.interfaces.AutonomousController;
import frc.robot.main.Pair;

public class AutonomousControllerImpl implements AutonomousController {
    ArrayList<Pair> speedMap = new ArrayList<Pair>(5);
    ArrayList<Pair> rotateMap = new ArrayList<Pair>(5);
    ArrayList<Pair> actionMap = new ArrayList<Pair>(25);

    public AutonomousControllerImpl() {
        speedMap.add(new Pair(1.0, 20)); // Speed value of 1.0 results in 20 inches/sec
        speedMap.add(new Pair(0.5, 10));
        speedMap.add(new Pair(0.25, 5));
        speedMap.add(new Pair(0.05, 1));

        rotateMap.add(new Pair(1.0, 30)); // rotate value of 1.0 results in 30 deg/sec
        rotateMap.add(new Pair(0.5, 15));
        rotateMap.add(new Pair(0.2, 5));
        rotateMap.add(new Pair(0.06, 1));
    }

    @Override
    public void setOperationList(String[] autoOp) {
        System.out.println("Received " + autoOp.length + " autoOp items");
        int time = 0;
        for (int i = 0; i < autoOp.length; i++) {
            System.out.println("Processing:" + autoOp[i]);
            Integer distangl = Integer.parseInt(autoOp[i].substring(5));
            boolean reverse = false;
            if (distangl < 0) {
                reverse = true;
                distangl = -distangl;
            }
            List<Pair> map = null;
            String action = null;
            if (autoOp[i].startsWith("Move")) {
                map = speedMap;
                action = "Move";
            } else {// if (autoOp[i].startsWith("Turn")) {
                map = rotateMap;
                action = "Turn";
            }
            for (Pair entPair : map) {
                // System.out.println("Checking "+entPair);
                if (entPair.p2 <= distangl) {
                    Integer duration = distangl / entPair.p2;
                    distangl = distangl % entPair.p2;
                    time += duration;
                    Pair p = new Pair(reverse ? -entPair.p1 : entPair.p1, time * 1000, action);
                    actionMap.add(p);
                    System.out.println("Adding " + p + ". Remaining:" + distangl);
                }
            }
        }
    }

    @Override
    public Pair getNextAction(long timeInAutonomous) {
        Pair chosenAction = null;
        Iterator<Pair> itr = actionMap.iterator();
        while (itr.hasNext()) {
            Pair action = itr.next();
            // System.out.println("Checking: "+action);
            if (timeInAutonomous-action.p2 >= -10) {
                System.out.println("Removing completed action:" + action);
                itr.remove();
            } else {
                chosenAction = action;
                break;
            }
        }
        return chosenAction;
    }

    @Override
    public Pair calibrate(long timeInTest) {
        return null;
    }
}