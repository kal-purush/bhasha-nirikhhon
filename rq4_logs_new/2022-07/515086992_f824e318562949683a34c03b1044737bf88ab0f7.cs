using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DieRoller
{
    /// <summary>
    /// Represents a single six sided die(1-6)
    /// </summary>
    public class Die
    {
        /// <summary>
        /// current face up value (What was rolled)
        /// </summary>
        public int FaceValue { get; set; }

        /// <summary>
        /// True if die is currently held
        /// </summary>
        public bool IsHeld { get; set; }
        
        

        /// <summary>
        /// Rolls the die and sets the <see cref="FaceValue"/>
        /// to the new number
        /// </summary>
        public byte Roll()
        {
            // Generate random number
            // Set to face value
            // Return new number

            throw new NotImplementedException();
        }
    }
}