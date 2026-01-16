using System;

namespace KiemTraNamNhuan
{
    class ktNamNhuan
    {
        static void Main(string[] args)
        {
            int year;
            Console.WriteLine("CT kiem tra nam nhuan !");
            Console.Write("Nhap nam : ");
            year = int.Parse(Console.ReadLine());
            if((year % 400) == 0)
            {
                Console.WriteLine("{0} la nam nhuan ", year);
            }else if ((year % 100) == 0)
            {
                Console.WriteLine("{0} khong phai la nam nhuan", year);
            }else if ((year % 4 ) == 0)
            {
                Console.WriteLine("{0} la nam nhuan", year);
            }else
            {
                Console.WriteLine("{0} khong phai la nam nhuan", year);
            }
            Console.ReadKey();
        }
    }
}