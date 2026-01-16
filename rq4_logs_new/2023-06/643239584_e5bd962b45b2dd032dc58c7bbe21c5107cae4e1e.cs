// Напишите программу которая принимает двоичное число, а выводит двоичный код этого числа 

int Imput (string message){
    Console.WriteLine(message);
    string peremen1 = Console.ReadLine();
    int peremen2 = Convert.ToInt32(peremen1);
    return peremen2;
}
// int x = Imput ("Введите число");
// string n = (" ");
// while (x>0)
// {
//     n = x%2+n;
//     x=x/2;
// }
// Console.WriteLine(n);




int x = Imput ("Введите число");
int n = 0;
int [] array = new int [x];
while (x>0)
{
    for (int i = 0; i < array.Length; i++)
    {
        n = x%2;
        array[i]=n;
        x=x/2;
    }
}

void PrintMassiv(int [] array)
{
    for (int i = 0; i < array.Length; i++)
    {
        Console.Write($"{array[i]}");
    }
}

PrintMassiv(array);