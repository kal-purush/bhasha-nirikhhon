using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace cscd349FinalProject
{
    class Player
    {
        private static Player _instance = null;
        private static List<ICharacter> _allies;

        public List<ICharacter> Allies
        {
            get { return _allies; }
            set
            {
                if(value.Count > 3)
                    throw new Exception("Too many characters for player's allies!");

                _allies = value;
            }
        }

        public static Player GetInstance()
        {
            if(_instance == null)
                _instance = new Player();

            return _instance;
        }
    }
}