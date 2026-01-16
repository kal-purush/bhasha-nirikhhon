Console.Clear();
Console.WriteLine("Введите любое число");
string number = (Console.ReadLine());

List<char> list = new List<char>(number);   //Создаем список, копируем в него строку

int count = 0;
for (int i = 0; i < list.Count; i++)        //Проверка нулей в старших разрядах числа
{   
    if ((int)Char.GetNumericValue(list[i]) == 0) count++;
    else break;
}
list.RemoveRange(0, count);



int len =  list.Count();
if (len < 3)
    Console.WriteLine("В числе нет третьей цифры нет!");

else
{
    char thirdDigit = list[2];
    Console.WriteLine(thirdDigit);
}



    