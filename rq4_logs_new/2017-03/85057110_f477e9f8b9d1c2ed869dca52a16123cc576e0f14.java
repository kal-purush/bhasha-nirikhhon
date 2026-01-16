package CustomListSorter;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by r3v3nan7 on 19.03.17.
 */
public class CustomList<T extends Comparable>{
    protected ArrayList<T> workingList;

    public ArrayList<T> getWorkingList(){
        return this.workingList;
    }

    public CustomList(){
        this.workingList = new ArrayList<T>();
    }

    public void add(T element){
        this.workingList.add(element);
    }

    public void remove(int element){
        this.workingList.remove(element);
    }

    public T getMax(){
        T max = workingList.get(0);
        for (int i = 1; i < workingList.size(); i++) {
            if(workingList.get(i).compareTo(max) > 0){
                max = workingList.get(i);
            }
        }

        return max;
    }


    public T getMin(){
        T min = workingList.get(0);
        for (int i = 1; i < workingList.size(); i++) {
            if(workingList.get(i).compareTo(min) < 0){
                min = workingList.get(i);
            }
        }

        return min;
    }


    public boolean contains(T element){
        return this.workingList.contains(element);
    }


    public void swap(int index1, int index2){
        T firstElement = this.workingList.get(index1);
        T secondElement = this.workingList.get(index2);

        this.workingList.set(index1, secondElement);
        this.workingList.set(index2, firstElement);

    }


    public int countGreaterThat(T element){
        int count = 0;

        for (int i = 0; i < this.workingList.size(); i++) {
            if(this.workingList.get(i).compareTo(element) > 0){
                count++;
            }
        }

        return count;
    }


    public void print(){
        //return this.workingList.toString();
        for (T t : workingList) {
            System.out.println(t);
        }
    }


    @SuppressWarnings("unchecked")
    public void sort(){
        Collections.sort(this.workingList);
    }


    @Override
    public String toString(){
        String str = "";

        for (T t : workingList) {
            str += "" + t + "\n";
        }

        return str;
    }


}