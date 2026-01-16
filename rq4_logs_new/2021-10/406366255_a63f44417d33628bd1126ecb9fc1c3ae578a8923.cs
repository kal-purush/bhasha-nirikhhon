namespace AutomatedCar.SystemComponents.Behaviour
{
    using AutomatedCar.SystemComponents.Packets;
    using ReactiveUI;

    public class Steering : SystemComponent
    {

        private SteeringPacket packet { get; }

        public SteeringLogic SL { get; } = new SteeringLogic();

        public Steering(VirtualFunctionBus virtualFunctionBus)
            : base(virtualFunctionBus)
        {
            this.packet = new SteeringPacket();
            this.virtualFunctionBus.SteeringPacket = this.packet;
        }

        /// <inheritdoc/>
        public override void Process()
        {
            this.packet.WheelPosition = this.SL.WheelPosition;
        }
    }
}