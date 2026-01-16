
using System;


string checkInput(string name)
{
    int num;
    bool isNum = false;
    string str = "";

    do
    {
        Console.WriteLine("Insert " + name + " please: ");
        str = Console.ReadLine();
        isNum = int.TryParse(str, out num);

        if (!isNum)
        {
            Console.WriteLine("Invalid input");
        }

    } while (!isNum);

    return str;
}
 

    int x = Convert.ToInt32(checkInput("x"));
    int y = Convert.ToInt32(checkInput("y"));
    
    int temp = y;

if (x > y)
{
    y = x;
    x = temp;
   
}
    
int sum = x;

if (x != y)
{
    temp = x;
    do
    {
        temp += 1;
        sum = sum + temp;
    }
    while (temp != y);

}
Console.WriteLine(sum);









