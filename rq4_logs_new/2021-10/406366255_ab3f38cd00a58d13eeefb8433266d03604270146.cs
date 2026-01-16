namespace AutomatedCar.SystemComponents.Behaviour
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using ReactiveUI;

    public class EngineRPM : ReactiveObject
    {
        private int rpm;

        public int RPM { get => this.rpm; set => this.RaiseAndSetIfChanged(ref this.rpm, value); }
    }
}