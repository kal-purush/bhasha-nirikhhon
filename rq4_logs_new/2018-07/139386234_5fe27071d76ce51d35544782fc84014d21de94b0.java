package algorithm;

import classes.*;

import java.util.ArrayList;

public class Find_by_distance {
    Matrix mat;
    Integer size;
    camera[] cameras;
    ArrayList<Integer> time;
    Log log;

    public Find_by_distance(String routefile,int size,String logfile){
        this.mat = new Matrix(routefile,size);
        this.log = new Log(logfile);
        this.size = size;
    }

    public ArrayList<Double> calc(){
        ArrayList<Double> ret = new ArrayList<Double>();
        for (int i = 0 ;i < this.size; i++){
            ret.add(0.0);
        }
        ArrayList<Double> freq = this.log.getFrequency(this.size);
        Integer recentId = this.log.getRecent();
        ArrayList<Integer> neigh = this.mat.getNeighbour(recentId);
        recentId --;
        for (int i = 0; i < this.size; i++){
            Double ratio = 0.0;
            Double tmpFreq = freq.get(i);

            Integer max = 100;
            Integer distance;
            if (neigh.contains(i+1)){
                distance = this.mat.getRoute()[recentId][i];
            }
            else{
                distance = max;
            }

            Double freqRatio = tmpFreq * 100 * 0.3;
            Double distRatio = (max - distance)/(double)max * 100 * 0.7;
            System.out.println(distRatio);

            ratio = freqRatio + distRatio;
            ret.set(i,ratio);


        }
        return ret;
    }

    public static void main(String[] args){
        Find_by_distance tmp = new Find_by_distance("D:\\Proj\\algo\\src\\main\\java\\classes\\route.txt",4,
                "D:\\Proj\\algo\\src\\main\\java\\classes\\log.txt");
        ArrayList<Double> i = tmp.calc();
        System.out.print(i);


    }

}