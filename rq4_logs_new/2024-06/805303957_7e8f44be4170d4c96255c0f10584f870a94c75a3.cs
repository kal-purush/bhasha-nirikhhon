using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

class Program 
{
    static void Main(string[] args)
    {
        /*
        Question 1: Read-Only Collections 
        */

        List<string> colorslist = new List<string>{"red", "green", "blue"};

        // Colors list converted to ReadOnly 
        ReadOnlyCollection<string> colors = new ReadOnlyCollection<string>(colorslist);

        /* attempt to add a new element to colorslist
        
        This throws an error since ReadOnlyCollection does not have an Add method, its only readonly    
         */
        colors.Add("yellow");

        // For loop to print contents of the list
        foreach (string color in colors)
        {
            Console.WriteLine("The colors: " + color);
        }
    }  
}



using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

class Program 
{
    static void Main(string[] args)
    {
        /*
        Question 2: Resizing a List Capacity 
        */

        List<int> number = new List<int>(100);

        // Add 10 intergers to list: numbers

        for(int i=0; i<10 ; i++)
        {
            number.Add(i);
        }
        // Print the Capacity and Count of the list
        Console.WriteLine("Capacity:" + number.Capacity);
        Console.WriteLine("Capacity:" + number.Count);

        
    }  
}



using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

class Program 
{
    static void Main(string[] args)
    {
        /*
        Question 3: Finding the Maximum Value in a List 
        */

        Random random = new Random(); // Initialise the random object
        List<int> numbers = new List<int>(); // Create an empty list of int type

        for (int i=0;i<10;i++)
        {   
            // Loop numbers list and add random numbers between 1 - 100
            numbers.Add(random.Next(1,100));
        }

        int max = numbers.Max(); //Find the Max value of 10 values randomly

        foreach (int num in numbers)
        {
            Console.WriteLine(num);
        }
        Console.WriteLine("Max Value: " + max);

        
    }  
}


using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

class Program 
{
    static void Main(string[] args)
    {
        /*
        Question 4: Merging Two Arrays
        */

        int[] array1 = { 1, 2, 3 };
        int[] array2 = { 4, 5, 6 };

        // Merging arrays
        int[] mergedArray = new int[array1.Length + array2.Length];
        array1.CopyTo(mergedArray, 0);
        array2.CopyTo(mergedArray, array1.Length);

        // Print the merged array
        foreach (int num in mergedArray)
        {
            Console.WriteLine(num);
        }
        
        
    }  
}



using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

class Book
    // Book class with attributes Title and Author with get:set property to read and write value (Title and Author) 
{
    public string Title {get;set;}
    public string Author {get;set;}
}
class Program 
{
    static void Main(string[] args)
    {
        /*
        Question 5: List of Custom Objects
        */

        //Create a list of book objects
        List<Book> booklist = new List<Book>();

        // Add books to the list
        booklist.Add(new Book{Title = "7 Powers", Author = "Hamilton Helmer"});
        booklist.Add(new Book{Title = "Malcolm Gladwell", Author = "Outliers"});
        booklist.Add(new Book{Title = "Good Strategy Bad Startegy", Author = "Richrd Rumelt"});

        foreach (Book book in booklist)
        {
            Console.WriteLine("Title: " + book.Title + ", Author: " + book.Author);
        }


        
    }  
}





using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

class Program 
{
    static void Main(string[] args)
    {
        /*
        Question 6: Sorting and Reversing an Array
        */

        int[] num_array = new int[] {3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5};

        Array.Sort(num_array); // Array.Sort method to sort from the min - max

        foreach (int num in num_array)
        {
            Console.WriteLine(num);
        }

        Console.WriteLine("--------------------------");

        Array.Reverse(num_array); // Array.Reverse method reorganise from Max - Min

        foreach (int arr in num_array)
        {
            Console.WriteLine(arr);
        }
    
    }  
}



using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

class Program 
{
    static void Main(string[] args)
    {
        /*
        Question 7: Create and Manipulate an Array
        */

        int[] just_numbers = new int[] {10, 20, 30, 40, 50};

        // Print the Length of just_numbers
        Console.WriteLine("The length of the array is: " + just_numbers.Length);

        // Change the 3rd element to 35
        just_numbers[2] = 35;

        // Print all elements in the array
        Console.WriteLine("Elements in the array:");
        foreach (int num in just_numbers)
        {
            Console.WriteLine(num);
        }
    }  
}


