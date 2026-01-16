using JustTimePass;
using System;
namespace GenericsDemo;
class MainClass
{
    public static void Main()
    {
        //array initialization
        int[] intArray = { 32, 45, 67 };
        double[] doubleArray = { 1.1, 2.2, 3.3 };
        char[] charArray = { 'h', 'e', 'l', 'l', 'o' };
        UCprintmax Obj = new UCprintmax();
        //Calling Array via simple methods
       // Obj.toPrint(intArray);
       // Obj.toPrint(doubleArray);
        //Obj.toPrint(charArray);
        //calling array via generics method
        Obj.toPrint<int>(intArray);
        Obj.toPrint<char>(charArray);
        Obj.toPrint<double>(doubleArray);

    }
}