using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlackPearlShip.Algorithms
{
    /// <summary>
    /// Determine how much Rum the crew should drink.
    /// Input:  time
    ///         did we hit or miss enemies ship (from WeaponAlgo)
    ///         type of food crew ate (from MealAlgo)
    /// Output: amount of rum to drink
    /// </summary>
    public class DrinkingAlgo
    {
        private ControlRoom ControlRoom { get; set; }

        public DrinkingAlgo(ControlRoom cr)
        {
            this.ControlRoom = cr;
        }
    }
}