package src.my.com.app;

import java.util.*;
    // Car pooling problem
// https://leetcode.com/problems/car-pooling/           
//
// Approach: Use a priority queue to track the events of passengers getting in and out of the car.
// For each trip, add an event for when passengers get in and another for when they get out. Poll the events in order and maintain the current capacity. If at any point the capacity exceeds the limit, return false. If all events are processed without exceeding capacity, return true.



class Solution {
    /* public boolean carPooling(int[][] trips, int capacity) {
        

        Map<Integer, Integer> sequence  = new TreeMap();

        for ( int i=0; i < trips.length; i++){
            // pasengers adding to capacity
            sequence.put(trips[i][1], sequence.getOrDefault( trips[i][1], 0 )+ trips[i][0] );

            // passengers getting down of the car
            sequence.put(trips[i][2], sequence.getOrDefault( trips[i][2], 0 ) - trips[i][0] );

        }

        int usedCapacity = 0;
        for ( int val : sequence.values()) {
            
           usedCapacity += val;
           if (usedCapacity > capacity ) return false;
        }

        return true;

    }*/

    public boolean carPooling(int[][] trips, int capacity) {


        PriorityQueue<int[]> seq  = new PriorityQueue<int[]>( (a,b) -> (a[0] - b[0]) == 0 ? a[1] - b[1] : a[0] - b[0] );

        for ( int i=0; i<trips.length; i++){
            // pasengers adding to capacity
            seq.add( new int[]{ trips[i][1] , trips[i][0] });

            // passengers getting down of the car
            seq.add(new int[]{ trips[i][2] , -1*trips[i][0] });

        }

        int usedCapacity = 0;
        while ( !seq.isEmpty()){
            usedCapacity += seq.poll()[1];
           if (usedCapacity > capacity ) 
                return false;
        }

        return true;
    }
}