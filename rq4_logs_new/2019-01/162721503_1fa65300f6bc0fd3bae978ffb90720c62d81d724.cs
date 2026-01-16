using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RJL.HW4.OOP.Classes.Task2
{
    class Agregat
    {
        public int AgregatCountDetails;
        public Agregat(int agregatCountDetails)
        {
            this.AgregatCountDetails = agregatCountDetails;
        }
       //public decimal getCountDaysForAssembly(int agregatCountDetails, int countDetailsPerDay)
       // {
       //     decimal temp = agregatCountDetails / countDetailsPerDay;
       //     decimal countDaysForAssembly = Math.Ceiling(temp);
       //     return countDaysForAssembly;
       // }
    }
}