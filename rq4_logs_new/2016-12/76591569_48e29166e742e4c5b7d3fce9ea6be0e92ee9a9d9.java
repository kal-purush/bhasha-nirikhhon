package com.cubeproblem.domain;

import java.util.List;

public class CubeProblem {

    int numTestCases;

    List<CubeIteration> iterations;

    public int getNumTestCases() {
        return numTestCases;
    }

    public void setNumTestCases(int numTestCases) {
        this.numTestCases = numTestCases;
    }

    public List<CubeIteration> getIterations() {
        return iterations;
    }

    public void setIterations(List<CubeIteration> iterations) {
        this.iterations = iterations;
    }

}