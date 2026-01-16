Console.WriteLine("Part 1\nEnter your number:");
string? number = Console.ReadLine();
char[] cNumber = number.ToCharArray();
string result = "";
for (int i = 0; i < number.Length; i++)
{
    if (i == number.Length - 1) //exeption: dont add space after last char(number)
    {
        result += cNumber[i];
    }
    else
    {
        result += cNumber[i] + " ";
    }
    
} //adding spaces between chars
Console.WriteLine(result);

int[] numbers = new int[5]; 

Console.WriteLine("Part 2\nEnter 5 numbers\n");
for (int i = 0;i < 5;i++)
{
    Console.WriteLine($"Enter your {i+1} number:");
    numbers[i] = Convert.ToInt32(Console.ReadLine());
} //adding numbers to array

int min = 0;
int max = 0;

for (int i = 0; i < numbers.Length; i++)
{
    if (numbers[min] > numbers[i])
    {
        min = i;
    }
} //finding min number
for (int i = 0; i < numbers.Length; i++)
{
    if (numbers[max] < numbers[i])
    {
        max = i;
    }
} //finding max number
Console.WriteLine($"Entered numbers: {numbers[0]}, {numbers[1]}, {numbers[2]}, {numbers[3]}, {numbers[4]}");
Console.WriteLine($"{numbers[max]}(max number) - {numbers[min]}(min number) = {numbers[max] - numbers[min]}");