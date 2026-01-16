// Пользователь вводит с клавиатуры M чисел. Посчитайте, сколько чисел больше 0 ввёл пользователь

Console.Write($"Введи количество чисел: ");
int m = Convert.ToInt32(Console.ReadLine());
int[] MasNum = new int[m];

void InputNumbers(int m){
for (int i = 0; i < m; i++)
  {
    Console.Write($"Введи {i+1} число: ");
    MasNum[i] = Convert.ToInt32(Console.ReadLine());
  }
}

int Comparison(int[] MasNum)
{
  int count = 0;
  for (int i = 0; i < MasNum.Length; i++)
  {
    if(MasNum[i] > 0 ) count += 1; 
  }
  return count;
}

InputNumbers(m);

Console.WriteLine($"Больше 0: {Comparison(MasNum)} ");