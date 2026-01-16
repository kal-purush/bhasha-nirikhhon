using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Windows.UI.Xaml.Controls;

namespace Pazaak
{
    public class AI : Player
    {
        
        public AI(Card[] crdPool, String name) : base(crdPool, name)
        {
          
        }

        public void mkMove(User u, TextBlock enScr, InGame iG, Random r)
        {
            //not the user's turn right now
            u.IsTrn = false;
            u.GotDk = false;
            if (TrnCnt < 9 && this.Stndng == false)
            {
                Card c = new Card(r);
                prcesCrd(iG, c);
                scnHnd(iG, u);
            }

            //be given a card 
            if (this.CurrScr > 16&& this.CurrScr<21)
            {
                this.Stndng = true;
            }
            else if(this.CurrScr>20)
            {
                this.IsBust = true;
            }
            enScr.Text = this.CurrScr.ToString();
            u.IsTrn = true;
        }

        private void prcesCrd(InGame iG, Card c)
        {   
            iG.enCrdSwtch(c.Val);
            addCrd(c);
            TrnCnt++;
            CurrScr += c.Val;
        }

        //scan the current hand to see if AI should use a card
        public void scnHnd(InGame iG,User u)
        {
            for(int hndLoop = 0; hndLoop<this.Hand.Length; hndLoop++ )
            {
                //make sure the selection wouldnt bust the AI
                if(this.CurrScr + this.Hand[hndLoop].Val < 21)
                {
                    //check if the card is a negative number
                    if(this.Hand[hndLoop].Val<0)
                    {
                        //use this card
                        useCrd(this.Hand, this.Hand[hndLoop], u, iG, hndLoop); 
                    }
                    //positive card
                    else
                    {
                        //if the card will make 20
                        if(this.CurrScr + this.Hand[hndLoop].Val == 20)
                        {
                            //use this card
                            useCrd(this.Hand, this.Hand[hndLoop], u, iG, hndLoop);
                        }
                        //if the user has stood, the AI only needs to beat their score now
                        if(u.Stndng)
                        {
                            //the enemy's score is better, they will win with this card
                            if(this.CurrScr + this.Hand[hndLoop].Val >u.CurrScr)
                            {
                                //use this score
                                useCrd(this.Hand, this.Hand[hndLoop], u, iG, hndLoop);
                            }
                        }
                        //keep cards unless needed, only use a card if they're currently losing
                        if(u.RndsWn>this.RndsWn)
                        {
                            if(this.CurrScr + this.Hand[hndLoop].Val >15)
                            {
                                //use card
                                useCrd(this.Hand, this.Hand[hndLoop], u, iG, hndLoop);
                            }
                        }
                    }
                }
            }
        }

        public void useCrd(Card[] arr, Card arrEle, User u, InGame iG, int elNum)
        {
            prcesCrd(iG,arrEle);
            arr = arr.Except(new Card[] { arrEle }).ToArray();
            iG.clrEnHndCrd(elNum);
        }
    }
}