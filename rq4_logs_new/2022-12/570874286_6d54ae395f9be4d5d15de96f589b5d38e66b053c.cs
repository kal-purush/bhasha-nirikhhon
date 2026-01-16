
int CountPosNum(){
int count = 0;
string word;
  while (true)
{
    Console.WriteLine("Введите число ");
    word = Console.ReadLine();
    if(word == "") return count;
    else if(int.Parse(word)>0) count++;
    Console.WriteLine($"Кол-во чисел больше 0 : {count}");
    
}  

}

Console.Write(CountPosNum());