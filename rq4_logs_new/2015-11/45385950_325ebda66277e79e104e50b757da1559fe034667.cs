using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AntWars.Board;
using AntWars.Board.Ants;

namespace AntWars.AI
{
    abstract class AIBase : IAI
    {

        private Player Player { get; set; }

        private Game Game { get; set; }

        public AIBase() { }

        private Base Base = null;


        protected void buyScout() {
            Scout s = new Scout();
            s.Owner = Player;
            // TODO währung abziehen
            BoardObject b = Game.Board.getBase(Player);
            s.Coords = b.Coords;
        }

        protected void buyCarrier() {

        }

        protected Base getBase()
        {
            if(Base == null)
                Base = Game.Board.getBase(Player);
            
            return Base;
        }

        public abstract void nextTick(int currentMoney);
        public abstract void antTick(Ant ant, List<BoardObject> view);

        public void nextTick()
        {
            nextTick(Player.money);
        }
    }
}