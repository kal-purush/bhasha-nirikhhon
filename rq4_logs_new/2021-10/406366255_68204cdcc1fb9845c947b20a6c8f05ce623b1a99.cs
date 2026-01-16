namespace AutomatedCar.SystemComponents.Behaviour
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using global::AutomatedCar.SystemComponents;

    /// <summary>
    /// An automated gearbox implementation.
    /// </summary>
    public class AutomaticGearbox : SystemComponent, IGearbox
    {
        /// <summary>
        /// The maximum numbers of the drive subgears.
        /// </summary>
        public const int MaxDriveSubgears = 5;

        /// <summary>
        /// Initializes a new instance of the <see cref="AutomaticGearbox"/> class.
        /// </summary>
        /// <param name="virtualFunctionBus">The car's virtual function bus.</param>
        public AutomaticGearbox(VirtualFunctionBus virtualFunctionBus)
            : base(virtualFunctionBus)
        {
            this.virtualFunctionBus = virtualFunctionBus;

            this.CurrentGear = Gear.Park;
            this.DriveSubgear = 0;
        }

        /// <inheritdoc/>
        public Gear CurrentGear { get; set; }

        /// <inheritdoc/>
        public int DriveSubgear { get; set; }

        /// <inheritdoc/>
        public double GetGearboxTorgue()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public override void Process()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public void ShiftDown()
        {
            switch (this.CurrentGear)
            {
                case Gear.Drive:
                    if (this.DriveSubgear == 0)
                    {
                        this.CurrentGear = Gear.Neutral;
                        break;
                    }

                    this.DriveSubgear = Math.Min(this.DriveSubgear - 1, 0);
                    break;
                case Gear.Neutral:
                    this.CurrentGear = Gear.Reverse;
                    break;
                case Gear.Reverse:
                    this.CurrentGear = Gear.Park;
                    break;
                case Gear.Park:
                    break;
            }
        }

        /// <inheritdoc/>
        public void ShiftUp()
        {
            switch (this.CurrentGear)
            {
                case Gear.Park:
                    this.CurrentGear = Gear.Reverse;
                    break;
                case Gear.Reverse:
                    this.CurrentGear = Gear.Neutral;
                    break;
                case Gear.Neutral:
                    this.CurrentGear = Gear.Drive;
                    break;
                case Gear.Drive:
                    this.DriveSubgear = Math.Min(this.DriveSubgear + 1, MaxDriveSubgears);
                    break;
            }
        }
    }
}