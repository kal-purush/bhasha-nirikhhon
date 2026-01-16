import processing.core.*; 
import processing.data.*; 
import processing.event.*; 
import processing.opengl.*; 

import java.util.HashMap; 
import java.util.ArrayList; 
import java.io.File; 
import java.io.BufferedReader; 
import java.io.PrintWriter; 
import java.io.InputStream; 
import java.io.OutputStream; 
import java.io.IOException; 

public class SortStep extends PApplet {

private int numInts = 800;
private int largestInt = 100;
private int [] myArray= new int[numInts];
private Stopwatch watch = new Stopwatch();
private Sorts mySorts = new Sorts();
public void setup()
{
  bubbleSortTest();
  selectionSortTest();
  insertionSortTest();
  mergeSortTest();
}
public void draw(){
  //empty!
}
public void selectionSortTest()
{
  System.out.println("Testing Selection Sort");
  fillArray(numInts, largestInt);
  watch.reset();
  watch.start();
  mySorts.selectionSort(myArray);
  watch.stop();
  System.out.println("Selection Sort took " +watch.elapsedTime()/1000 + " microseconds");
}
public void bubbleSortTest()
{
  System.out.println("Testing Bubble Sort");
  fillArray(numInts, largestInt);
  watch.reset();
  watch.start();
  mySorts.bubbleSort(myArray);
  watch.stop();
  System.out.println("Bubble Sort took " +watch.elapsedTime()/1000 + " microseconds");
}
public void insertionSortTest()
{
  System.out.println("Testing Insertion Sort");
  fillArray(numInts, largestInt);
  watch.reset();
  watch.start();
  mySorts.insertionSort(myArray);
  watch.stop();
  System.out.println("Insertion Sort took " +watch.elapsedTime()/1000 + " microseconds");
}
public void mergeSortTest()
{
  System.out.println("Testing Merge Sort");
  fillArray(numInts, largestInt);
  watch.reset();
  watch.start();
  mySorts.mergeSort(myArray,0,myArray.length-1);
  watch.stop();
  System.out.println("Merge Sort took " +watch.elapsedTime()/1000 + " microseconds");
}
private void fillArray(int numInts, int largestInt)
{
  for (int loop = 0; loop < myArray.length; loop++)
  {
    myArray[loop] = (int)(Math.random()*largestInt + 1);
  }
}

  static public void main(String[] passedArgs) {
    String[] appletArgs = new String[] { "SortStep" };
    if (passedArgs != null) {
      PApplet.main(concat(appletArgs, passedArgs));
    } else {
      PApplet.main(appletArgs);
    }
  }
}