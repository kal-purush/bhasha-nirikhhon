// Задача итоговая. Написать программу, которая из имеющегося массива строк формирует массив из строк,
// длинна которых меньше или ровна трём символам.

string[] myArray = {"Hello", "yes", "#$%", "Sergey", "my", "int", "log", "var", "s"};
for(int i =0; i < myArray.Length; i++)
    Console.Write(myArray[i] + " ");

string[] myArray2 = new string[myArray.Length];
for(int i = 0; i < myArray.Length; i++)
{
    if(myArray[i].Length <= 3)
    {
        myArray2[i] = myArray[i];
    }
}
Console.WriteLine();
for(int i = 0; i < myArray2.Length; i++)
    Console.Write(myArray2[i] + " ");
     