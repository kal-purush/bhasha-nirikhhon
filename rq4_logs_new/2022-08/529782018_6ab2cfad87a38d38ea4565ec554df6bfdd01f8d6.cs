// Подсчитать сумму цифр в числе
Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("Введите число ");
int number = int.Parse(Console.ReadLine());
//---------------------------------------------------------------
void sum_digits_of_the_number(int num)
{
    int result = 0,temp_number = num;
    while (temp_number>0)
    {
        result += temp_number%10;
        temp_number/=10;
        // Console.WriteLine(result);
    }
    Console.WriteLine($"Сумма цифр в числе {number} равна {result}");
}
sum_digits_of_the_number(number);