import java.util.*;

class Mutator extends Thread {
  boolean loop = true;
  
  public void run() {
    while ( loop ) {
      Vector v = new Vector();
      // mutator
      for (int k = 0; k < 1000; k++) {
	Vector e = new Vector();
	for (int l = 0; l < 10000; l++) {
	  e.addElement(new Integer(l));
	}
	Thread.yield();
	v.addElement(e);
      }
      //
      System.out.print('\b'+"-");
      v = null;
    }
  }
    
  public void end() {
    loop = false;
  }
}

class Sort extends Thread {
  static int arr[];
  static long time;

  public static void print_arr(){
    for (int i = 0; i< arr.length; i+=300) {
      System.out.print(arr[i]);
    }
    System.out.println(" " + (System.currentTimeMillis()-time) + " ms");
    time = System.currentTimeMillis();
  }

  static void scramble() {
    int a[] = new int[21000];
    double f = 10.0 / (double) a.length;
    for (int i = a.length; --i >= 0;) {
      a[i] = (int)(i * f);
    }
    for (int i = a.length; --i >= 0;) {
      int j = (int)(i * Math.random());
      int t = a[i];
      a[i] = a[j];
      a[j] = t;
    }
    arr = a;
  }

  static void sort(int a[]) throws Exception {
    int y = 0;
    for (int i = a.length; --i>=0; ) {
      boolean swapped = false;
      for (int j = 0; j<i; j++) {
	if (a[j] > a[j+1]) {
	  int T = a[j];
	  a[j] = a[j+1];
	  a[j+1] = T;
	  swapped = true;
	}
      }
      Thread.yield();
      if ( i % 50 == 0 ) print_arr();
      if (!swapped)
	return;
    }
  }
  public void run(){
    System.out.println("** start Sort *****");
    scramble();
    print_arr();
    try {
      sort(arr);
    } catch(Exception e) {
    }
    print_arr();
    System.out.println("** done Sort ********");
  }
}
	
class GCTest {

  public static void main(String[] args) {
    Sort sort = new Sort();
    Mutator mutator = new Mutator();
    sort.start();
    mutator.start();
    try {
      sort.join();
      mutator.end();
      sort.print_arr();
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}
