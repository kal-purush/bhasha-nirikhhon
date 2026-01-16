using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.Ports;

namespace NeuLog.Barometer
{
    public class BarometerSerial : IBarometerSerial, IDisposable
    {
        private SerialPort Port { get; set; }
        private string PortName { get; set; }

        public BarometerSerial(string portName)
        {
            PortName = portName;
        }

        private void Open()
        {
            Port = new SerialPort(PortName, 115200, Parity.None, 8, StopBits.Two);
            Port.Open();
        }


        public async Task<bool> IsDeviceActive()
        {            
            try
            {
                Open();

                await Port.BaseStream.WriteAsync(Encoding.ASCII.GetBytes("UNeuLog!"), 0, 8);

                byte[] result = new byte[8];
                int bytesRead = 0;
                while (bytesRead < 8)
                    bytesRead += await Port.BaseStream.ReadAsync(result, 0, 8 - bytesRead);

                var resultString = Encoding.Default.GetString(result);

                if (resultString.StartsWith("OK-V

                return false;
            }
            finally
            {
                Close();
            }
        }

        public async Task<decimal> GetBarometerValue()
        {            
            try
            {
                Open();

                await Port.BaseStream.WriteAsync(new byte[] { 0x55, 0x12, 0x01, 0x31, 0, 0, 0, 0x99 }, 0, 8);

                byte[] result = new byte[8];
                int bytesRead = 0;
                while (bytesRead < 8)
                    bytesRead += await Port.BaseStream.ReadAsync(result, 0, 8 - bytesRead);

                return ConvertBytesToVarometer(result);
            }
            finally
            {
                Close();
            }
        }

        private decimal ConvertBytesToVarometer(byte[] data)
        {
            return (decimal)data[4] + (((decimal)data[6]) / 100M);
        }

        private void Close()
        {
            Port?.Close();
        }

        public void Dispose()
        {
            Close();
        }
    }
}