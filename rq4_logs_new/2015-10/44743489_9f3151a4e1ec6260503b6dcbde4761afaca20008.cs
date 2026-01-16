using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheMoneyDrop
{
    public class GameController
    {
        private GameView view;

        private MoneydropController moneyDrop;

        public GameController ()
        {
            view = new GameView(this);

            moneyDrop = new MoneydropController(this);

           // moneyDrop.AantalTrapdoors = 4;
            moneyDrop.initialize();
        }

        public GameView getView()
        {
            return view;
        }
    }
}