// 1번 문제

package JAVA_LAB.week4;

public class Counter {
    private int counter;

    public Counter(int counter1){
        counter = counter1;
    }

    void up(){
        counter++;
    }

    public int getCount(){ return counter; }
    public void setCount( int counter ){ this.counter = counter; }
}