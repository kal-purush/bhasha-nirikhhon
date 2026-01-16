using PiLed.Devices.Implementations;
using PiLed.Display;
using PiLed.Models;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static PiLed.Display.RainbowColorDisplay;

namespace PiLed
{
    public class DemoMode
    {
        private readonly List<IPixelDisplay> _displays;
        private int _demoDisplayTime = 30000;

        public DemoMode(IPixelDevice device)
        {
            _displays = new List<IPixelDisplay>();
            _displays.Add(new ThrobDisplay(device, new PixelColor(0, 1, 1)));
            _displays.Add(new RainbowColorDisplay(device, RainbowType.Led));
            _displays.Add(new ThrobDisplay(device, new PixelColor(120, 1, 1)));
            _displays.Add(new RainbowColorDisplay(device, RainbowType.ColorWheel));
            _displays.Add(new ThrobDisplay(device, new PixelColor(240, 1, 1)));
            _displays.Add(new RainbowColorDisplay(device, RainbowType.Led));
        }

        public void Start()
        {
            var iterator = 0;

            while (true)
            {

                var tokenSource = new CancellationTokenSource();
                var token = tokenSource.Token;
                var task = Task.Run(() => _displays[iterator].Start(token), token);

                Thread.Sleep(_demoDisplayTime);

                tokenSource.Cancel();
                iterator = (iterator + 1) % _displays.Count;
            }
        }
    }
}