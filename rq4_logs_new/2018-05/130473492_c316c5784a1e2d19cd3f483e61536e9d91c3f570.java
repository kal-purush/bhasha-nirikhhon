/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import localsearch.model.IConstraint;
import localsearch.model.IFunction;
import localsearch.model.VarIntLS;
import localsearch.search.MoveType;
import localsearch.search.OneVariableValueMove;

import localsearch.selectors.MinMaxSelector;
import util.UtilManager;

/**
 *
 * @author DungBachViet
 */
public class MyLocalSearch {

    /**
     * @param args
     */
    private java.util.Random rand = null;
    private double t_best;
    private double t0;
    private double t;

    public MyLocalSearch() {
        rand = new java.util.Random();
    }

    public double getTimeBest() {
        return t_best * 0.001;
    }

    public void TwoStagesGreedy(IConstraint S, int currentTest) {
        // Tham lam 2 buoc de triet tieu vi pham rang buoc
        System.out.println("Init S = " + S.violations());
        MinMaxSelector mms = new MinMaxSelector(S);

        ArrayList<Double> iterationViolation = new ArrayList<Double>();
        int it = 0;
        while (it < 10000 && S.violations() > 0) {

            VarIntLS sel_x = mms.selectMostViolatingVariable();
            int sel_v = mms.selectMostPromissingValue(sel_x);

            sel_x.setValuePropagate(sel_v);
            System.out.println("Step " + it + " " + ", S = " + S.violations());

            it++;
            iterationViolation.add((double) S.violations());
        }

        System.out.println(S.violations());
        ExamineTimetablingProblem.violationList.add(currentTest, iterationViolation);
    }

    public double calculateFitness(IFunction[] f) {
        return 0.25 * (f[0].getValue() / ExamineTimetablingProblem.maxExamGap
                - ExamineTimetablingProblem.minDisproportion / f[1].getValue()
                + f[2].getValue() / ExamineTimetablingProblem.maxSuitableTimeSlot
                + f[3].getValue() / ExamineTimetablingProblem.maxDistributeDifficulty);
    }

    public double calculateFitness(IFunction[] f, int delta1, int delta2, int delta3, int delta4) {
        return 0.25 * ((f[0].getValue() + delta1) / ExamineTimetablingProblem.maxExamGap
                - ExamineTimetablingProblem.minDisproportion/(f[1].getValue() + delta2)
                + (f[2].getValue() + delta3) / ExamineTimetablingProblem.maxSuitableTimeSlot
                + (f[3].getValue() + delta4) / ExamineTimetablingProblem.maxDistributeDifficulty);
    }

    public void myTabuSearchMaintainConstraints(IFunction[] f, IConstraint S,
            int tabulen, int maxTime, int maxIter, int maxStable, int currTest) {
        double t0 = System.currentTimeMillis();

        VarIntLS[] x = S.getVariables();
        HashMap<VarIntLS, Integer> map = new HashMap<VarIntLS, Integer>();
        for (int i = 0; i < x.length; i++) {
            map.put(x[i], i);
        }

        int n = x.length;
        int maxV = -1000000000;
        int minV = 100000000;
        for (int i = 0; i < n; i++) {
            if (maxV < x[i].getMaxValue()) {
                maxV = x[i].getMaxValue();
            }
            if (minV > x[i].getMinValue()) {
                minV = x[i].getMinValue();
            }
        }
        int D = maxV - minV;
        int tabu[][] = new int[n][D + 1];
        for (int i = 0; i < n; i++) {
            for (int v = 0; v <= D; v++) {
                tabu[i][v] = -1;
            }
        }

        int it = 0;
        maxTime = maxTime * 1000;// convert into milliseconds

//        int best = f.getValue();
        double best = calculateFitness(f);
        int[] x_best = new int[x.length];
        for (int i = 0; i < x.length; i++) {
            x_best[i] = x[i].getValue();
        }

        System.out.println("TabuSearch, init S = " + S.violations());
        int nic = 0;
        ArrayList<OneVariableValueMove> moves = new ArrayList<OneVariableValueMove>();
        Random R = new Random();

        ArrayList<Double> iterationFitness = new ArrayList<Double>();

        while (it < maxIter && System.currentTimeMillis() - t0 < maxTime) {
            int sel_i = -1;
            int sel_v = -1;
            int minDelta = 10000000;
            moves.clear();
            for (int i = 0; i < n; i++) {
                for (int v = x[i].getMinValue(); v <= x[i].getMaxValue(); v++) {
                    int deltaS = S.getAssignDelta(x[i], v);

                    int deltaF1 = f[0].getAssignDelta(x[i], v);
                    int deltaF2 = f[1].getAssignDelta(x[i], v);
                    int deltaF3 = f[2].getAssignDelta(x[i], v);
                    int deltaF4 = f[3].getAssignDelta(x[i], v);

                    double newQuality = calculateFitness(f, deltaF1, deltaF2, deltaF3, deltaF4);

                    if (deltaS <= 0) {
                        if (tabu[i][v - minV] <= it
                                || newQuality > best) {
                            if (newQuality > best) {
                                best = newQuality;
                                sel_i = i;
                                sel_v = v;
                                moves.clear();
                                moves.add(new OneVariableValueMove(
                                        MoveType.OneVariableValueAssignment,
                                        newQuality, x[i], v));
                            } else if (newQuality == best) {
                                moves.add(new OneVariableValueMove(
                                        MoveType.OneVariableValueAssignment,
                                        newQuality, x[i], v));
                            }
                        }
                    }
                }
            }

            // perform the move
            if (moves.size() <= 0) {
                System.out.println("TabuSearch::restart.....");
                restartMaintainConstraint(x, S, tabu);
                nic = 0;
            } else {
                OneVariableValueMove m = moves.get(R.nextInt(moves.size()));
                sel_i = map.get(m.getVariable());
                sel_v = m.getValue();
                x[sel_i].setValuePropagate(sel_v);
                tabu[sel_i][sel_v - minV] = it + tabulen;

                double currentQuality = calculateFitness(f);
                System.out.println("Step " + it + ", S = " + S.violations()
                        + ", f = " + currentQuality + ", best = " + best
                        + ", nic = " + nic);
                // update best
                if (currentQuality > best) {
                    best = currentQuality;
                    for (int i = 0; i < x.length; i++) {
                        x_best[i] = x[i].getValue();
                    }
                    updateBest();
                    t_best = System.currentTimeMillis() - t0;
                }

                //if (minDelta >= 0) {
                if (currentQuality <= best) {
                    nic++;
                    if (nic > maxStable) {
                        System.out.println("TabuSearch::restart.....");
                        restartMaintainConstraint(x, S, tabu);
                        nic = 0;
                    }
                } else {
                    nic = 0;
                }
            }
            it++;

            iterationFitness.add((double) best);

        }
        for (int i = 0; i < x.length; i++) {
            x[i].setValuePropagate(x_best[i]);
        }

        ExamineTimetablingProblem.fitnessList.add(currTest, iterationFitness);
        ExamineTimetablingProblem.bestFitness.add((double) best);

    }

    private void restartMaintainConstraint(VarIntLS[] x, IConstraint S,
            int[][] tabu) {

        for (int i = 0; i < x.length; i++) {
            java.util.ArrayList<Integer> L = new java.util.ArrayList<Integer>();
            for (int v = x[i].getMinValue(); v <= x[i].getMaxValue(); v++) {
                if (S.getAssignDelta(x[i], v) <= 0) {
                    L.add(v);
                }
            }
            int idx = rand.nextInt(L.size());
            int v = L.get(idx);
            x[i].setValuePropagate(v);
        }
        for (int i = 0; i < tabu.length; i++) {
            for (int j = 0; j < tabu[i].length; j++) {
                tabu[i][j] = -1;
            }
        }

    }

    public void updateBest() {

    }

    public void myTabuSearch(IConstraint S, int tabulen, int maxTime, int maxIter,
            int maxStable) {
        double t0 = System.currentTimeMillis();

        VarIntLS[] x = S.getVariables();
        HashMap<VarIntLS, Integer> map = new HashMap<VarIntLS, Integer>();
        for (int i = 0; i < x.length; i++) {
            map.put(x[i], i);
        }

        int n = x.length;
        int maxV = -1000000000;
        int minV = 1000000000;
        for (int i = 0; i < n; i++) {
            if (maxV < x[i].getMaxValue()) {
                maxV = x[i].getMaxValue();
            }
            if (minV > x[i].getMinValue()) {
                minV = x[i].getMinValue();
            }
        }
        int D = maxV - minV;
        // System.out.println("n = " + n + ", D = " + D);
        int tabu[][] = new int[n][D + 1];
        for (int i = 0; i < n; i++) {
            for (int v = 0; v <= D; v++) {
                tabu[i][v] = -1;
            }
        }

        int it = 0;
        maxTime = maxTime * 1000;// convert into milliseconds

        int best = S.violations();
        int[] x_best = new int[x.length];
        for (int i = 0; i < x.length; i++) {
            x_best[i] = x[i].getValue();
        }

        System.out.println("TabuSearch, init S = " + S.violations());
        int nic = 0;
        ArrayList<OneVariableValueMove> moves = new ArrayList<OneVariableValueMove>();
        Random R = new Random();
        while (it < maxIter && System.currentTimeMillis() - t0 < maxTime
                && S.violations() > 0) {
            int sel_i = -1;
            int sel_v = -1;
            int minDelta = 10000000;
            moves.clear();
            for (int i = 0; i < n; i++) {
                for (int v = x[i].getMinValue(); v <= x[i].getMaxValue(); v++) {
                    int delta = S.getAssignDelta(x[i], v);
                    // System.out.println("min  =   "+x[i].getMinValue()+"   max =     "+x[i].getMaxValue());
                    /*
					 * Accept moves that are not tabu or they are better than
					 * the best solution found so far (best)
                     */
                    if (tabu[i][v - minV] <= it
                            || S.violations() + delta < best) {
                        if (delta < minDelta) {
                            minDelta = delta;
                            sel_i = i;
                            sel_v = v;
                            moves.clear();
                            moves.add(new OneVariableValueMove(
                                    MoveType.OneVariableValueAssignment,
                                    minDelta, x[i], v));
                        } else if (delta == minDelta) {
                            moves.add(new OneVariableValueMove(
                                    MoveType.OneVariableValueAssignment,
                                    minDelta, x[i], v));
                        }
                    }
                }
            }

            if (moves.size() <= 0) {
                System.out.println("TabuSearch::restart.....");
                restartMaintainConstraint(x, S, tabu);
                if (S.violations() == 0) {
                    best = S.violations();
                    for (int i = 0; i < x.length; i++) {
                        x_best[i] = x[i].getValue();
                    }
                }
                // restart(x,tabu);
                nic = 0;
            } else {
                // perform the move
                OneVariableValueMove m = moves.get(R.nextInt(moves.size()));
                sel_i = map.get(m.getVariable());
                sel_v = m.getValue();
                x[sel_i].setValuePropagate(sel_v);
                tabu[sel_i][sel_v - minV] = it + tabulen;

                System.out.println("Step " + it + ", S = " + S.violations()
                        + ", best = " + best + ", delta = " + minDelta
                        + ", nic = " + nic);
                // update best
                if (S.violations() <= best) {
                    best = S.violations();
                    for (int i = 0; i < x.length; i++) {
                        x_best[i] = x[i].getValue();
                    }
                    updateBest();
                    t_best = System.currentTimeMillis() - t0;
                    nic = 0;
                } //if (minDelta >= 0) {
                else if (S.violations() >= best) {
                    nic++;
                    if (nic > maxStable) {
                        System.out.println("TabuSearch::restart.....");
                        restartMaintainConstraint(x, S, tabu);
                        nic = 0;
                    }
                }
            }
            it++;
        }
        for (int i = 0; i < x.length; i++) {
            x[i].setValuePropagate(x_best[i]);
        }
        System.out.println("TabuSearch, init S = " + S.violations());
    }

    public void myAnnealingSearchMaintainConstraints(IFunction[] f, IConstraint S,
            int maxTime, int maxIter, double initialTemp, double endingTemp, int currTest) {

        // Get the initial time
        double t0 = System.currentTimeMillis();

        // Get list of varialbes and create HashMap from Variable to Integer
        VarIntLS[] x = S.getVariables();
        HashMap<VarIntLS, Integer> map = new HashMap<VarIntLS, Integer>();
        for (int i = 0; i < x.length; i++) {
            map.put(x[i], i);
        }

        // Get number of variables
        int n = x.length;

        // Convert into milliseconds
        maxTime = maxTime * 1000;

        // Save the best of solution over time
        double best = calculateFitness(f);
        int[] x_best = new int[x.length];
        for (int i = 0; i < x.length; i++) {
            x_best[i] = x[i].getValue();
        }

        // Set up current temperature to initial temperature
        double currentTemp = initialTemp;

        // Speed for descending temperature per iteration
        double descendingSpeed = 1 - (Math.log(initialTemp) - Math.log(endingTemp)) / maxIter;

        System.out.println("Annealing Search , init S = " + S.violations() + ", best = " + best + ", Initial Temperature : " + currentTemp);

        Random R = new Random();
        ArrayList<Double> iterationFitness = new ArrayList<Double>();
        int it = 0;
        while (it < maxIter && System.currentTimeMillis() - t0 < maxTime) {

            // Choose the variable and its value randomly
            int sel_i = UtilManager.randomInt(0, n-1);
            int sel_v = UtilManager.randomInt(x[sel_i].getMinValue(), x[sel_i].getMaxValue());

            // Check if constraints are be maintaining to 0
            int deltaS = S.getAssignDelta(x[sel_i], sel_v);
            if (deltaS <= 0) {

                // Get changes of each objectives
                int deltaF1 = f[0].getAssignDelta(x[sel_i], sel_v);
                int deltaF2 = f[1].getAssignDelta(x[sel_i], sel_v);
                int deltaF3 = f[2].getAssignDelta(x[sel_i], sel_v);
                int deltaF4 = f[3].getAssignDelta(x[sel_i], sel_v);

                // Get new quality of neighbor
                double newQuality = calculateFitness(f, deltaF1, deltaF2, deltaF3, deltaF4);

                if (newQuality > best) {
                    x[sel_i].setValuePropagate(sel_v);
                    best = newQuality;
                    for (int i = 0; i < x.length; i++) {
                        x_best[i] = x[i].getValue();
                    }

                } else if (Math.random() <= Math.exp((newQuality - best) / currentTemp)) {
                    x[sel_i].setValuePropagate(sel_v);
                }

                System.out.println("Iter : " + it + ", S = " + S.violations() + ", Fitness = " + newQuality + ", best = " + best + ", Current Temperature : " + currentTemp);
                currentTemp = currentTemp * descendingSpeed;
                it++;
            }

            iterationFitness.add((double) best);
        }

        for (int i = 0; i < x.length; i++) {
            x[i].setValuePropagate(x_best[i]);
        }

        ExamineTimetablingProblem.fitnessList.add(currTest, iterationFitness);
        ExamineTimetablingProblem.bestFitness.add((double) best);

    }

    public void myDegratedCeilingSearchMaintainConstraints(IFunction[] f, IConstraint S,
            int maxTime, int maxIter, int currTest) {

        // Get the initial time
        double t0 = System.currentTimeMillis();

        // Get list of varialbes and create HashMap from Variable to Integer
        VarIntLS[] x = S.getVariables();
        HashMap<VarIntLS, Integer> map = new HashMap<VarIntLS, Integer>();
        for (int i = 0; i < x.length; i++) {
            map.put(x[i], i);
        }

        // Get number of variables
        int n = x.length;

        // Convert into milliseconds
        maxTime = maxTime * 1000;

        // Save the best of solution over time
        double best = calculateFitness(f);
        int[] x_best = new int[x.length];
        for (int i = 0; i < x.length; i++) {
            x_best[i] = x[i].getValue();
        }

        double currentCeiling = calculateFitness(f);
        double descendingSpeed = (1 - currentCeiling) / maxIter;

        System.out.println("Degrated Ceiling, init S = " + S.violations() + ", best = " + best + ", Initial Ceiling : " + currentCeiling);

        Random R = new Random();
        ArrayList<Double> iterationFitness = new ArrayList<Double>();
        int it = 0;
        while (it < maxIter && System.currentTimeMillis() - t0 < maxTime) {

            // Choose the variable and its value randomly
            int sel_i = UtilManager.randomInt(0, n-1);
            int sel_v = UtilManager.randomInt(x[sel_i].getMinValue(), x[sel_i].getMaxValue());

            // Check if constraints are be maintaining to 0
            int deltaS = S.getAssignDelta(x[sel_i], sel_v);
            if (deltaS <= 0) {

                // Get changes of each objectives
                int deltaF1 = f[0].getAssignDelta(x[sel_i], sel_v);
                int deltaF2 = f[1].getAssignDelta(x[sel_i], sel_v);
                int deltaF3 = f[2].getAssignDelta(x[sel_i], sel_v);
                int deltaF4 = f[3].getAssignDelta(x[sel_i], sel_v);

                // Get new quality of neighbor
                double newQuality = calculateFitness(f, deltaF1, deltaF2, deltaF3, deltaF4);

                if (newQuality > best) {
                    x[sel_i].setValuePropagate(sel_v);
                    best = newQuality;
                    for (int i = 0; i < x.length; i++) {
                        x_best[i] = x[i].getValue();
                    }

                } else if (newQuality > currentCeiling) {
                    x[sel_i].setValuePropagate(sel_v);
                }
                
                System.out.println("Iter : " + it + ", S = " + S.violations() + ", Fitness = " + newQuality + ", best = " + best + ", Current Ceiling : " + currentCeiling);
                currentCeiling = currentCeiling + descendingSpeed;
                it++;
            }

            iterationFitness.add((double) best);
        }

        for (int i = 0; i < x.length; i++) {
            x[i].setValuePropagate(x_best[i]);
        }

        ExamineTimetablingProblem.fitnessList.add(currTest, iterationFitness);
        ExamineTimetablingProblem.bestFitness.add((double) best);
    }

    public void myHillClimbingSearchMaintainConstraints(IFunction[] f, IConstraint S,
            int maxTime, int maxIter, int currTest) {

        // Get the initial time
        double t0 = System.currentTimeMillis();

        // Get list of varialbes and create HashMap from Variable to Integer
        VarIntLS[] x = S.getVariables();
        HashMap<VarIntLS, Integer> map = new HashMap<VarIntLS, Integer>();
        for (int i = 0; i < x.length; i++) {
            map.put(x[i], i);
        }

        // Get number of variables
        int n = x.length;

        // Convert into milliseconds
        maxTime = maxTime * 1000;

        // Save the best of solution over time
        double best = calculateFitness(f);
        int[] x_best = new int[x.length];
        for (int i = 0; i < x.length; i++) {
            x_best[i] = x[i].getValue();
        }

        System.out.println("Degrated Ceiling, init S = " + S.violations() + ", best = " + best);

        Random R = new Random();
        ArrayList<Double> iterationFitness = new ArrayList<Double>();
        int it = 0;
        while (it < maxIter && System.currentTimeMillis() - t0 < maxTime) {

            // Choose the variable and its value randomly
            int sel_i = UtilManager.randomInt(0, n-1);
            int sel_v = UtilManager.randomInt(x[sel_i].getMinValue(), x[sel_i].getMaxValue());

            // Check if constraints are be maintaining to 0
            int deltaS = S.getAssignDelta(x[sel_i], sel_v);
            if (deltaS <= 0) {

                // Get changes of each objectives
                int deltaF1 = f[0].getAssignDelta(x[sel_i], sel_v);
                int deltaF2 = f[1].getAssignDelta(x[sel_i], sel_v);
                int deltaF3 = f[2].getAssignDelta(x[sel_i], sel_v);
                int deltaF4 = f[3].getAssignDelta(x[sel_i], sel_v);

                // Get new quality of neighbor
                double newQuality = calculateFitness(f, deltaF1, deltaF2, deltaF3, deltaF4);

                if (newQuality >= best) {
                    x[sel_i].setValuePropagate(sel_v);
                    best = newQuality;
                    for (int i = 0; i < x.length; i++) {
                        x_best[i] = x[i].getValue();
                    }

                }
                
                System.out.println("Iter : " + it + ", S = " + S.violations() + ", Fitness = " + newQuality + ", best = " + best);
                it++;
            }

            iterationFitness.add((double) best);
        }

        for (int i = 0; i < x.length; i++) {
            x[i].setValuePropagate(x_best[i]);
        }

        ExamineTimetablingProblem.fitnessList.add(currTest, iterationFitness);
        ExamineTimetablingProblem.bestFitness.add((double) best);
    }

    
    public void myImprovingTabuSearchMaintainConstraints(IFunction[] f, IConstraint S,
            int tabulen, int maxTime, int maxIter, int maxStable, int currTest) {
        double t0 = System.currentTimeMillis();
        
        VarIntLS[] x = S.getVariables();
        HashMap<VarIntLS, Integer> map = new HashMap<VarIntLS, Integer>();
        for (int i = 0; i < x.length; i++) {
            map.put(x[i], i);
        }

        int n = x.length;
        int maxV = -1000000000;
        int minV = 100000000;
        for (int i = 0; i < n; i++) {
            if (maxV < x[i].getMaxValue()) {
                maxV = x[i].getMaxValue();
            }
            if (minV > x[i].getMinValue()) {
                minV = x[i].getMinValue();
            }
        }
        int D = maxV - minV;
        int tabu[][] = new int[n][D + 1];
        for (int i = 0; i < n; i++) {
            for (int v = 0; v <= D; v++) {
                tabu[i][v] = -1;
            }
        }

        int it = 0;
        maxTime = maxTime * 1000;// convert into milliseconds

//        int best = f.getValue();
        double best = calculateFitness(f);
        int[] x_best = new int[x.length];
        for (int i = 0; i < x.length; i++) {
            x_best[i] = x[i].getValue();
        }

        System.out.println("TabuSearch, init S = " + S.violations());
        int nic = 0;
        ArrayList<OneVariableValueMove> moves = new ArrayList<OneVariableValueMove>();
        Random R = new Random();

        ArrayList<Double> iterationFitness = new ArrayList<Double>();

        while (it < maxIter && System.currentTimeMillis() - t0 < maxTime) {
            
            // Choose the variable and its value randomly
            int sel_i = UtilManager.randomInt(0, n-1);
            int sel_v = UtilManager.randomInt(x[sel_i].getMinValue(), x[sel_i].getMaxValue());

            // Check if constraints are be maintaining to 0
            int deltaS = S.getAssignDelta(x[sel_i], sel_v);
            if (deltaS <= 0) {
                
                // Get changes of each objectives
                int deltaF1 = f[0].getAssignDelta(x[sel_i], sel_v);
                int deltaF2 = f[1].getAssignDelta(x[sel_i], sel_v);
                int deltaF3 = f[2].getAssignDelta(x[sel_i], sel_v);
                int deltaF4 = f[3].getAssignDelta(x[sel_i], sel_v);

                // Get new quality of neighbor
                double newQuality = calculateFitness(f, deltaF1, deltaF2, deltaF3, deltaF4);
 
                // If neighbor don't belong to Tabu List || newQuality > best
                if (tabu[sel_i][sel_v - minV] <= it
                        || newQuality > best) {
                    // Update nic (number of not improving successive iterations)
                    if (newQuality <= best)
                        nic++;
                    else if (newQuality > best)
                        nic = 0;
                    
                    // Update the best solution
                    if (newQuality >= best) {
                        x[sel_i].setValuePropagate(sel_v);
                        best = newQuality;
                        for (int i = 0; i < x.length; i++) {
                            x_best[i] = x[i].getValue();
                        }
                        tabu[sel_i][sel_v - minV] = it + tabulen;
                    }
                }

                
                if (nic >= maxStable) {
                    System.out.println("TabuSearch::restart.....");
                    restartMaintainConstraint(x, S, tabu);
                    nic = 0;
                }
                    
                System.out.println("Iter : " + it + ", S = " + S.violations() + ", Fitness = " + newQuality + ", best = " + best + ", nic = " + nic);
                it++;
            }
            
            iterationFitness.add((double) best);
        }
        
        for (int i = 0; i < x.length; i++) {
            x[i].setValuePropagate(x_best[i]);
        }

        ExamineTimetablingProblem.fitnessList.add(currTest, iterationFitness);
        ExamineTimetablingProblem.bestFitness.add((double) best);
    }

}