// Program.cs file, C# code.
// Task: From the array of strings entered from the console, the new array should be
// formed containing strings whose length is not more than three characters
//
int FillArray(string[] array) // To fill the array of strings by entering them from the console
                              // and calculate number of short strings.
{
    int sizeNew = 0; // initial value of size for new array with short strings
    Console.WriteLine("Please enter all strings of this array separated by <ENTER>");
    for (int index = 0; index < array.Length; index++)
    {
        array[index]=Console.ReadLine();
        if(array[index].Length <= 3) sizeNew += 1;
    }
    return sizeNew; // transfer value of size for new array with short strings
}
//
string[] ReformArray(string[] array, int sizeNew) // To reform the array by checking length of strings.
                                                  // Here sizeNew is the size of new array with short strings.
{
    string[] arrayNew = new string[sizeNew]; // To create new array.
    int indexNew = 0; 
    for (int index = 0; index < array.Length; index++)
    {
        if(array[index].Length <= 3)
        {
            arrayNew[indexNew] = array[index];
            indexNew += 1;
        }
    }
    return arrayNew;
}
//
void WriteArray(string[] array){ // To write all strings of the array
    int size = array.Length;
    int indexLast = size - 1;
    Console.Write("[");
    for(int index = 0; index < size; index++){
        Console.Write(array[index]);
        if(index < indexLast) Console.Write(" , ");
    }
    Console.Write("]");
}
//
//
Console.Write("Please enter size of the string array: ");
int size = Convert.ToInt32(Console.ReadLine());
string[] array1 = new string[size];
int size2 = FillArray(array1);
string[] array2 = ReformArray(array1, size2);
WriteArray(array1);
Console.Write(" -> ");
WriteArray(array2);