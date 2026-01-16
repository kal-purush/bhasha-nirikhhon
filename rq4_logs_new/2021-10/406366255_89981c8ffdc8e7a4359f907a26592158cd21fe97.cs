namespace AutomatedCar.SystemComponents.Behaviour
{
    using System;
    using AutomatedCar.SystemComponents.Packets;

    public class DummyPedals : SystemComponent
    {
        private DummyPedalPacket dummyPedalPacket;

        private int gaspedal = 0;
        private int breakpedal = 0;

        public DummyPedals(VirtualFunctionBus virtualFunctionBus)
            : base(virtualFunctionBus)
        {
            this.dummyPedalPacket = new DummyPedalPacket();
            this.virtualFunctionBus.ReadonlyDummyPedalPacket = this.dummyPedalPacket;
        }

        public void GaspedalUp()
        {
            this.gaspedal += 100 / 60;
            this.breakpedal = 0;
            this.dummyPedalPacket.Gaspeadl = this.gaspedal;
            this.dummyPedalPacket.Brakepedal = this.breakpedal;
        }

        public void BreakpedalUp()
        {
            this.breakpedal += 100 / 60;
            this.gaspedal = 0;
            this.dummyPedalPacket.Gaspeadl = this.gaspedal;
            this.dummyPedalPacket.Brakepedal = this.breakpedal;
        }

        public override void Process()
        {
            throw new NotImplementedException();
        }
    }
}