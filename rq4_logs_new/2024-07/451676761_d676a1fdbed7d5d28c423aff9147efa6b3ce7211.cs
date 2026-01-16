using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;

/*
You are given a 0-indexed integer array mapping which represents the mapping rule of a shuffled decimal system. 
mapping[i] = j means the digit i should be mapped to digit j in this system.

The mapped value of an integer is the new integer obtained by replacing each occurrence of gigit i in the integer with mapping [i] for all 0<= i <= 9

you are also given another integer array nums.
return the array nums sorted in non-decreasing order based on the mapped values of its elements


so...............
we are given an array of numbers
we change the number values in accordance to the given mapping array
then evaluate and sort the original array based off of the new "value" 
but return the old values / just sorted by the mapped value
custom weights?

can't use a sorted array or any data type that sorts via key/pair because these sort based off of the key
the key must be unique
in this case, the key is not necessarily unique and breaks all of those solutions

my solution is to use an object to store the mapped value and the original value
I can then sort by the mapped value even if it isn't unique since I have full control over the sort
since the object stores both values, the values are always connected
so I can easily sort by either one and print the other and have them be connected
so I sort by the mapped value and then print the original value
which causes the original value to be maintained and sorted appropriately

tested with both examples and it works 



improvements..
better sort
 surely O(n^2) is not the most efficient way to sort a custom object list
 different container?

do you really need to use an object? 

beter way to convert value -> mapped value?
 - currently converting value to a string
 - mapping each char of that string to the new value one by one
 - then stitching it back together and reverting to int
seems rather.. barbaric?



*/
public class sortByCustomMapping{

	public sortByCustomMapping(){}

    public void solve() {

        //given variables
        int[] mapping = { 4, 7, 2, 1, 9, 0, 6, 3, 8, 5 };
        int[] nums = { 789, 456, 123, 612, 5, 92, 925, 792 };

        /*
        //TEST2
        int[] mapping = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int[] nums = { 789, 456, 123 };
        */

        //additonal variables
        List<ValuePair> valuePairs = new List<ValuePair>();

        //instatiate object array
        for (int i = 0; i < nums.Length; i++) {
            valuePairs.Add(new ValuePair(nums[i]));
        }

        //generate mapped values (weights)
        for (int i = 0; i < nums.Length; i++) {
            string temp = nums[i].ToString();
            string final = "";
            foreach (char c in temp) {
                final += mapping[int.Parse(c.ToString())]; //LMAO  
            }
            valuePairs[i].setMappedValue(int.Parse(final));
        }

        //Sort object list
        //O(n^2) probably not the best sort..
        Boolean changed = true;
        ValuePair hold = new ValuePair();
        while (changed) {
            changed = false;
            for (int i = 0; i < valuePairs.Count - 1; i++) {
                if (valuePairs[i].getMappedValue() > valuePairs[i + 1].getMappedValue()) {
                    hold = valuePairs[i];
                    valuePairs[i] = valuePairs[i + 1];
                    valuePairs[i + 1] = hold;
                    changed = true;
                }
            }
        }

        //PRINT FINAL RESULT
        Console.WriteLine("Original values..");
        for (int i = 0; i < nums.Length; i++) {
            Console.Write(nums[i]+" ");
        }
        Console.WriteLine("\n");


        Console.WriteLine("Sorted by custom mapping..");
        for (int i = 0; i < nums.Length; i++) {
            Console.Write(valuePairs[i].getInitialValue() + " ");
        }
        Console.WriteLine("\n\n\n");

        Console.WriteLine("Sorted mapped values.. (for validation)");
        for (int i = 0; i < nums.Length; i++) {
            Console.Write(valuePairs[i].getMappedValue() + " ");
        }
        Console.WriteLine("\n\n\n");

    }
}

//Object class to store value and mapped value
public class ValuePair {
    int initialValue;
    int mappedValue;

    public ValuePair() {
        initialValue = 0;
        mappedValue = 0;
    }

    public ValuePair(int a) {
        initialValue = a;
        mappedValue = 0;
    }

    //set functions
    public void setMappedValue(int a) {
        mappedValue = a;
    }

    //get functions
    public int getInitialValue() {
        return initialValue;
    }

    public int getMappedValue() {
        return mappedValue;
    }
}