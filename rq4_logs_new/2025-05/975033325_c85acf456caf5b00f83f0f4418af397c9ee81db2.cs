namespace Practice2.Task18
{
    internal class Program
    {
        static void arrayDelete(int[] myArray, out int[] newArray, out int n)
        {
            int i = 0;
            n = 0;
            newArray = new int[0];
            foreach (var item in myArray)
            {
                if (item < 0)
                {
                    myArray = myArray.Where(e => e != item).ToArray();
                    n++;
                }
                i++;
            }
            newArray= myArray.ToArray();

            //return myArray;
        }
        static void Main(string[] args)
        {
            int[] Arr = { 1, 2, -5, 3, 4, -1 };
            arrayDelete(Arr, out int[] newArray, out int n);
            int [] newArr= newArray.OfType<int>().ToArray();
            int negativeNumbers = n;
            foreach (var item in newArr)
            {
                Console.WriteLine(item);
            }

            Console.WriteLine($"Количество отрицательных значений в массиве {negativeNumbers}");
        }
    }
}