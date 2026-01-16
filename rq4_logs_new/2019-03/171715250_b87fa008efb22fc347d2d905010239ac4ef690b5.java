package org.team5.app.main;
/* Author: Holden D
 * DataAnalzyer
 *  Is used for tracking and calculating statistical data point by point
 */
 
public class DataAnalyzer{
    private int count;
    private long mean;
    private long max;
    private long min;
        
    public DataAnalzer(){}
    
    public void writeData(long timeIn, long timeOut){
        count++;
        //latency
        long latency = timeOut-timeIn;
        
        //update running mean
        mean  *= (count-1)/count + (latency/count) ;
        
        //update max
        if(latency > max){
            max = lantency;
        }
    }
    
    public void printStats(){
        System.out.println(String.format("Average Latency: %.9f\nMax Latency: %.9f", mean, max));
    }
}