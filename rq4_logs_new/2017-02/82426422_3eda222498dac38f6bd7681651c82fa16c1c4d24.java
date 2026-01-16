package KNN.files;

import java.io.Serializable;

/**
 * Created by lucasmsp on 20/02/17.
 */
public class Candidate implements Serializable{

    double[] labels = new double[1];
    double[] distances = new double[1];
    int K =1;


    public Candidate() {
    }

    public void setK(int K){
        this.K = K;
        labels = new double[K+1];
        distances = new double[K+1];
    }

    public int getK() {
        return K;
    }

    public int getSize(){
        return labels.length;
    }

    public double[] getLabels() {
        return labels;
    }

    public void setLabels(double[] labels) {
        this.labels = labels;
    }

    public double[] getDistances() {
        return distances;
    }

    public void setDistances(double[] distances) {
        this.distances = distances;
    }
    public void setDistance(int i,double d){
        distances[i]=d;
    }
    public void setLabel(int i,double l){
        labels[i]=l;
    }
}