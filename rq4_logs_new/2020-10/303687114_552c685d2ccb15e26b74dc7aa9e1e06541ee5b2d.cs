using GPIOProjects.Base;
using System;
using System.Device.Gpio;
using System.Threading;

namespace GPIOProjects
{
    public class LEDBlink : BaseProject
    {
        private const int _pin = 18;
        private const int _sleepTime = 500;

        public LEDBlink(GpioController controller) : base(controller) { }

        protected override void Run()
        {
            Console.WriteLine("Running LEDBlink.cs");

            Console.WriteLine("Press any key to stop or wait 10 seconds...");
            var cancellationTokenSource = new CancellationTokenSource(new TimeSpan(hours: 0, minutes: 0, seconds: 10));
            do
            {
                Console.WriteLine($"LED On for {_sleepTime}ms");
                _controller.Write(_pin, PinValue.High);

                Thread.Sleep(_sleepTime);

                Console.WriteLine($"LED Off for {_sleepTime}ms");
                _controller.Write(_pin, PinValue.Low);

                Thread.Sleep(_sleepTime);
            } while (!Console.KeyAvailable && !cancellationTokenSource.Token.IsCancellationRequested);
        }

        protected override void Startup()
        {
            if (!_controller.IsPinOpen(_pin))
                _controller.OpenPin(_pin, PinMode.Output);
            else if (_controller.GetPinMode(_pin) != PinMode.Output)
                _controller.SetPinMode(_pin, PinMode.Output);
        }
    }
}