int[] Array(int n){
    int[] array = new int[n];
    for (int i = 0; i < array.Length; i++){
        array[i] = new Random().Next(100,999);
    }
        return array;
}

int[] Print (int[] arr){
    for (int i = 0; i <arr.Length; i++){
        Console.Write($"{arr[i]} ");
    Console.WriteLine();    
    }
    return arr;
}

int ReadNumber(string message){
    Console.Write(message);
    var s = Console.ReadLine();
    int a = s == null? 0 : int.Parse(s);
    return a;
}

int EvenNums(int[] arr){
    int count = 0;
    for (int i = 0; i < arr.Length; i++){
        if(arr[i]%2==0){
            count++;
        }
    }
     return count;
}

int n = ReadNumber("Введите размер массива ");
Console.WriteLine(EvenNums(Print(Array(n))));