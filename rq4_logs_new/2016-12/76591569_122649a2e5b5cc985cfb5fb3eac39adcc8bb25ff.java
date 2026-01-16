package com.cubeproblem.domain;

import java.util.List;

public class CubeIteration {

    int n;
    int m;
    List<CubeOperation> cubeOperations;

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public int getM() {
        return m;
    }

    public void setM(int m) {
        this.m = m;
    }

    public List<CubeOperation> getCubeOperations() {
        return cubeOperations;
    }

    public void setCubeOperations(List<CubeOperation> cubeOperations) {
        this.cubeOperations = cubeOperations;
    }

}