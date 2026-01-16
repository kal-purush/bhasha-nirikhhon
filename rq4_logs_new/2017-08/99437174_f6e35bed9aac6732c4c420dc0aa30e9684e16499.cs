using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TextReport
{
    class Program
    {
        static void Main(string[] args)
        {
            const string path = @"C:\Users\Misha\Desktop\Звіт.txt";

            string text = "1; Samsung Galaxy S8; Смартфон; 21.05.2016; необмежено; 12; 25.230; Samsung; 380(44)3905333; 12.06.2017;" +
                          " Склад №3; 5.8, Super AMOLED, (2960х1440), 4 x 2.3 ГГц + 4 x 1.7 ГГц; Нормальний телефон\n";

            TextReport rep = new TextReport();
            rep.SetInfo(text);
            rep.PrintTextReport();

        }

    }
}
