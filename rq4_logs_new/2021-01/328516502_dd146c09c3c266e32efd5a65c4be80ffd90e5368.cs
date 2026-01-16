using PiLed.Devices.Implementations;
using PiLed.Models;
using System.Threading;

namespace PiLed.Display
{
    /// <summary>
    /// Interface referring to a particular effect or illusion that can be displayed by a PixelLed(s)
    /// </summary>
    public interface IPixelDisplay
    {

        /// <summary>
        /// Represent the specific pixel device that htis display will manipulate
        /// </summary>
        IPixelDevice _pixelDevice { get; set; }

        /// <summary>
        /// Begins the displays main logic routine for this display type
        /// </summary>
        void Start(CancellationToken token);

        void Flush(params PixelBuffer[] buffers)
        {
            _pixelDevice.FlushColorToLeds(buffers);
        }
    }
}