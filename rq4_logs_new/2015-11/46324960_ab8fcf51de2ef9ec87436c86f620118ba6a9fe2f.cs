using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HighLow
{
    class Card
    {
        //PROPERTY
        private int _num;
        public int Num
        {
            get { return _num; }
            set
            {
                if (value > 1 && value < 15)
                    _num = value;
                else Console.WriteLine("Error : number is wrong.");
            }
        }
        private int _suit;
        public int Suit
        {
            get { return _suit; }
            set
            {
                if (value >= 0 && value < 4)
                    _suit = value;
                else Console.WriteLine("Error : suit is wrong.");
            }
        }
        public string[] suitName = new string[4] {"Club","Diamond","Heart","Spade"};
        //METHOD
        public Card(int suitTmp,int numTmp)
        {
            Suit = suitTmp;
            Num = numTmp;
        }
        public override string ToString()
        {
            string s = ""+suitName[Suit]+" ";
            switch(Num)
            {
                case 14:
                    s += "Ace";
                    break;
                case 11:
                    s += "Jack";
                    break;
                    case 12:
                        s += "Queen";
                        break;
                    case 13:
                        s += "King";
                        break;
                    default:
                        s += "" + Num + "";
                        break;

            }
            return s;
        }
    }
}