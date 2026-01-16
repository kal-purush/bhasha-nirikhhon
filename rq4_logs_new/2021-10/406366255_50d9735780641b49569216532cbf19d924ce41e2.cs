namespace AutomatedCar.SystemComponents.Behaviour
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// The behaviour of a pedal.
    /// </summary>
    public class Pedal
    {
        /// <summary>
        /// The maximum value of a pedal.
        /// </summary>
        public const double MaxPedalPosition = 100;

        /// <summary>
        /// Gets or sets the state of the pedal (0-100).
        /// </summary>
        public double Position { get; set; }

        /// <summary>
        /// Increases the gas.
        /// </summary>
        public void Increase()
        {
            this.Position = Math.Min(this.Position + 1, MaxPedalPosition);
        }

        /// <summary>
        /// Increases the gas.
        /// </summary>
        public void Decrease()
        {
            this.Position = Math.Max(this.Position - 1, 0);
        }
    }
}