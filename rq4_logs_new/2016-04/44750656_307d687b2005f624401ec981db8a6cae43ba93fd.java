/**
 * Created by quang on 4/5/16.
 */

import com.sun.deploy.util.ArrayUtil;

import java.util.Collections;
import java.util.*;

/**
 * Playground - a place to test my code and play around
 */
public class Playground {
    public static void main(String args[]){
        ArrayList<Integer> myList = new ArrayList<Integer>();
        myList.add(1);
        myList.add(2);
        myList.add(3);

        Collections.reverse(myList);

        System.out.println(myList);



    }
}