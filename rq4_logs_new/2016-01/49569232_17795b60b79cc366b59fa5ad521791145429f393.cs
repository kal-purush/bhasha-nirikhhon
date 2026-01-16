using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pazaak
{
    class User : Player
    {
        public User(Card[] crdPool, String name) : base(crdPool, name)
        {

        }
        public void tkTurn(Card c, Boolean stnd)
        {
            if (stnd)
            {
                this.Stndng = true;
            }
            if (TrnCnt < 9 && this.Stndng == false)
            {
                addCrd(c);
                TrnCnt++;
                CurrScr += c.Val;
                
            }
        }
    }
}