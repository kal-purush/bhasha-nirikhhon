using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatternFive.Behavioral.ChainofResponsibility
{
    public static class ClientCodeForChainOfResponsibility
    {
        public static void UseChainOfRespo(long requestedAmount)
        {
            TwoThousandHandler twoThousandHandler = new TwoThousandHandler();
            FiveHounderdHandler fiveHunderdhandler = new FiveHounderdHandler();
            TwoHundredHandler twoHundredHandler = new TwoHundredHandler();
            HundredHandler hundredHandler = new HundredHandler();

            twoThousandHandler.SetNextHandler(fiveHunderdhandler);
            fiveHunderdhandler.SetNextHandler(twoHundredHandler);
            twoHundredHandler.SetNextHandler(hundredHandler);

          
            if(requestedAmount % 100==0)
            {
                twoThousandHandler.DispatchMoney(requestedAmount);

            }
            else
            {
                Console.WriteLine("You enter invalid amount:{0}",requestedAmount);
            }
        }
    }
}