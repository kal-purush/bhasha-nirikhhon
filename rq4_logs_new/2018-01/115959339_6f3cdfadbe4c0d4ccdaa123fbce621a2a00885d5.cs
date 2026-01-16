using Entities;

namespace Administration
{
    internal class PortDeviceProvider : Port<IDevice>,  IDeviceProvider
    {
        private IDevice _device;

        public PortDeviceProvider(Port<IDevice> port)
        {
            port?.Connect(this);
        }

        public IDevice GetDevice()
        {
            return _device;
        }

        protected override void PostReceive(IDevice data)
        {
        }

        protected override void PostTransfer(IDevice data)
        {
        }

        protected override void PreReceive(IDevice data)
        {
            _device = data;
        }

        protected override void PreTransfer(IDevice data)
        {
        }
    }

}