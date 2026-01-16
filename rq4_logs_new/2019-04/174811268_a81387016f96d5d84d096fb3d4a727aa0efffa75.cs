using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simulator
{
    class PassportControll
    {
        public bool Visa;
        public int PasspotrData;

        public PassportControll()
        {
        }

        public PassportControll(int aPasspotrData)
        {
           
            PasspotrData = aPasspotrData;
        }

        public PassportControll(bool aVica,int aPasspotrData)
        {
            Visa = aVica;
            PasspotrData = aPasspotrData;
        }


        public void ControllPassport(int PasspotrData, bool Visa)
        {
            string yearsNow = DateTime.Now.ToString("yy");
            int year = Convert.ToInt32(yearsNow);
            if ((PasspotrData - year) > 10 && Visa == true)
            {
                Console.WriteLine($"\nПроходите дальше.Задекларируйте товар, если есть что декларировать");
            }
            else if (Visa == false)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nБез визы ехать нельзя.");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nСо старым паспортом ехать нельзя.");
            }      
        }

        public void ControllPassport(int PasspotrData)
        {
            string yearsNow = DateTime.Now.ToString("yy");
            int year = Convert.ToInt32(yearsNow);
            if ((PasspotrData - year) > 10)
            {
                Console.WriteLine($"\nПроходите дальше.Задекларируйте товар, если есть, что декларировать.");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nСо старым паспортом ехать нельзя.");
            }
        }

        public bool ControllPassportTrue (int PasspotrData, bool aVica)
        {
            string yearsNow = DateTime.Now.ToString("yy");
            int year = Convert.ToInt32(yearsNow);
            if ((PasspotrData - year) > 10 && aVica== true )
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool ControllPassportTrue(int PasspotrData)
        {
            string yearsNow = DateTime.Now.ToString("yy");
            int year = Convert.ToInt32(yearsNow);
            if ((PasspotrData - year) > 10)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}