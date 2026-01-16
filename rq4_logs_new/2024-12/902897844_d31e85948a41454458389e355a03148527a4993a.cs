// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");
int a = 5;
int b = 6;
String str = "TEst test test test";

int fact = 5;
Console.WriteLine(countFactorial(fact));


NewMethod(a, b);
countProbils(str);

static void NewMethod(int a, int b)
{
    int sum  = a + b;
    Console.WriteLine(sum);
  
}

static void countProbils(String str) {
    int length = str.Length;
    for (int i = 0; i < length; i++) {
        if (str[i] == ' ')
            Console.WriteLine("Space found by index " + i);
        }
    }

static int countFactorial(int fact)
{
    if (fact == 0) {
        return 1;
    }
    return fact * countFactorial(fact - 1);
}

Console.WriteLine("Enter your surname: ");
string surname = Console.ReadLine();
Console.WriteLine("Enter your name: ");
string name = Console.ReadLine();
Console.WriteLine("Hello, " + name + "!" + " " + surname);