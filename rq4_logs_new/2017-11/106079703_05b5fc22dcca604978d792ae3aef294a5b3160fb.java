import java.util.*;
import java.io.*;
public class NewTest {
static int min,max;
static int arr[]=new int[7];
static Scanner sc=new Scanner(System.in);
public static void input()
{
    min=0;
    max=6;
    genRandom();
}
public static int genRandom() {
    // generate random int value between 1 and 7
    int range = (max - min) + 1;     
    int random= (int)(Math.random() * range) + min;
    return random;
}

    public static boolean containsDuplicates() 
    {
    // check to see if positions have matching values
    for (int i = 0; i < 7; i++) 
        { 
        for (int j = i + 1 ; j < 7; j++) 
        { 
            if (arr[i]==arr[j]) 
            { // got the duplicate element } } }
            //System.out.println("Duplicates Exist");
        // if matching value is found, randomize the array again
        shuffle();
        return true;
			}
		}
	}
		//System.out.println("No duplicates");
    int n = arr.length;
    for (int i = 0; i < arr.length; i++) {
        // Get a random index of the array past i.
        int random = i + (int) (Math.random() * (n - i));
        // Swap the random element with the present element.
        int randomElement = arr[random];
        arr[random] = arr[i];
        arr[i] = randomElement;
    }
    System.out.println();
    /*for (int i = 0; i < arr.length; i++)
    {
        System.out.print(arr[i]+" ");
    }*/
    System.out.println();
    return false;
}

public static void shuffle() {
    arr[0]=min;
    arr[1]=max;
    for (int i = 2; i < arr.length; i++) {
        arr[i] = genRandom();
        //System.out.println(arr[i]);
    }
    containsDuplicates();
}

public static void main(String[] args)throws Exception {
	String s="";
	PrintWriter out = new PrintWriter(new FileWriter("filename.java"));
	String arr1[]= {"class Name","{","}","public static void main(String args[])","{","System.out.println(\"Hello\")","}"};
    //while(true)
    for(int j=0;j<10;j++)
    {
    System.out.println();
    NewTest ob=new NewTest();
    ob.input();
    //int[] cards = new int[7];
    shuffle();
    for(int i =0;i<7;i++)
	{
		//out.println(arr1[arr[i]]);
		System.out.println(arr1[arr[i]]);
	}
	out.close();
	
	/*try
	{
	Runtime.getRuntime().exec("javac filename.java");
	}
	catch(Error e)
	{
		s=e.toString();
	}
	if(s=="")
	{
		System.out.println("COMPILED ");
	break;
}
		
   }*/
   
}
}
}