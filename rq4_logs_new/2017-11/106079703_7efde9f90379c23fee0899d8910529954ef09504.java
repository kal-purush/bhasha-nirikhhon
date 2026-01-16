import java.util.*;
public class MeisterLiga {
static int min,max;
static Scanner sc=new Scanner(System.in);
public static void input()
{
    System.out.print("Enter the number of stars of the player (Press 99 to exit) - ");
    double stars=sc.nextDouble();
    if(stars==3) { min=-2; max=10;genRandom(); }
    else if(stars==3.5) { min=0; max=13; genRandom(); }
    else if(stars==4) { min=4; max=20;genRandom(); }
    else if(stars==4.5) {min=5; max=23;genRandom(); }
    else if(stars==5) {min=5; max=25;genRandom(); }
    else if(stars==99) {System.out.println("Goodbye !"); System.exit(6); }
    else { System.out.println("Wrong Choice !\n"); input();}
}
public static int genRandom() {
    int range = (max - min) + 1;     
    int random= (int)(Math.random() * range) + min;
    return random;
}

    public static boolean containsDuplicates(int[] arr) 
    {
    for (int i = 0; i < 10; i++) 
        { 
        for (int j = i + 1 ; j < 10; j++) 
        { 
            if (arr[i]==arr[j]) 
            { 
        shuffle(arr);
        return true;
    }
}
}
    int n = arr.length;
    for (int i = 0; i < arr.length; i++) {
        int random = i + (int) (Math.random() * (n - i));
        int randomElement = arr[random];
        arr[random] = arr[i];
        arr[i] = randomElement;
    }
    System.out.println();
    for (int i = 0; i < arr.length; i++)
    {
        System.out.print(arr[i]+" ");
    }
    System.out.println();
    return false;
}

public static void shuffle(int[] arr) {
    arr[0]=min;
    arr[1]=max;
    for (int i = 2; i < arr.length; i++) {
        arr[i] = genRandom();
    }
    containsDuplicates(arr);
}

public static void main(String[] args) {
    System.out.println("------ MeisterLiga Slot Generator ------");
    while(true)
    {
    System.out.println();
    MeisterLiga ob=new MeisterLiga();
    ob.input();
    int[] cards = new int[10];
    shuffle(cards);
   }
}
}