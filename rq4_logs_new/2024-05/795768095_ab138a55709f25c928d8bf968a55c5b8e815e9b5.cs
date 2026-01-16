using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class Money : IMoney
    {
        public int MainPart { get; private set; }
        public int CoinPart { get; private set; }

        public Money(int mainPart, int subPart)
        {
            SetMoney(mainPart, subPart);
        }

        public void SetMoney(int mainPart, int subPart)
        {
            if (mainPart < 0 || subPart < 0)
                throw new ArgumentException("Main part or sub part can't be negative");
            if (subPart >= 100)
            {
                mainPart += subPart / 100;
                subPart %= 100;
            }
            MainPart = mainPart;
            CoinPart = subPart;
        }

        public override string ToString()
        {
            return $"{MainPart}.{CoinPart:D2}";
        }
    }
}