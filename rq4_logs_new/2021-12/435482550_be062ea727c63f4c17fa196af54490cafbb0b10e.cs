using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImplicitDeclarationOrnek
{
    class Program
    {
        static void Main(string[] args)
        {
            //İzmir'de bomba yedim ve tanesi 5 liraydı ve hava sıcaklığı 25,5 dereceydi.
            string sehir = "Ankara";
            string yiyecek = "lahmacun";
            int fiyat = 10;
            float havaSicakligi = 27.4f;
            Console.WriteLine("{0}'da {1} yedim ve tanesi {2} liraydı ve hava sıcaklığı {3} dereceydi",sehir, yiyecek, fiyat, havaSicakligi);
            Console.ReadLine();
        }
    }
}