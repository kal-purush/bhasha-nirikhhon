package Algorithms;

import IO.Options;
import Map.UrbanMap;

public class AlgorithmManager {
    UrbanMap map;
    GeneticAlgorithm geneticAlgorithm;
    HillClimb hillClimb;

    Options options;

    public AlgorithmManager(Options options, UrbanMap map) {
        this.options = options;
        this.map = map;

        this.geneticAlgorithm = new GeneticAlgorithm();
        this.hillClimb = new HillClimb();
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    public void run() {
        if (options.isUsingHillClimb());
            // do something with hill climb
        else; // do something with GA
    }

    public Metrics getMetrics() {
        return new Metrics(); // return the actual metrics from the correct algorithm
    }

}