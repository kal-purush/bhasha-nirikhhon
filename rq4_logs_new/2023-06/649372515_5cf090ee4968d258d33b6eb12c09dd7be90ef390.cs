Console.Write("Enter Array Length: ");
int.TryParse(Console.ReadLine(), out var aLength);
int[] numbersArray = new int[aLength];
for (int cntr = 0; cntr < aLength; cntr++)
{
    Console.Write($"Enter {cntr + 1} element of array: ");
    numbersArray[cntr] = Convert.ToInt32(Console.ReadLine());
}
Console.Write("\nEnter number to find in your array: ");
var nToFind = Convert.ToInt32(Console.ReadLine());
var nToFindResult = -1;
for (int cntr = 0; cntr < aLength; cntr++)
{
    if (numbersArray[cntr] == nToFind)
        nToFindResult = cntr;
}
if (nToFindResult != -1)
    Console.WriteLine($"Index of Number that  looking for is: [{nToFindResult}] number is - {numbersArray[nToFindResult]}");
else 
    Console.WriteLine("There's no such Number in array!!!");
//Array.Reverse(numbersArray, 0, aLength);
int temp = 0;
for (int cntr = 0; cntr <= aLength/2; cntr++)
{
    temp = numbersArray[cntr];
    numbersArray[cntr] = numbersArray[aLength - cntr - 1];
    numbersArray[aLength - cntr - 1] = temp;
}
var result = "\nYour Array is:";
for (int cntr = 0; cntr < aLength; cntr++)  
    result += $"  {numbersArray[cntr]}";

Console.WriteLine(result);