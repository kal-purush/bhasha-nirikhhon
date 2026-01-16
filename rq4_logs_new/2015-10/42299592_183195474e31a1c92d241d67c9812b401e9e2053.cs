using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net.Sockets;
using System.Timers;
using GalaSoft.MvvmLight.Ioc;
using System.Data.Linq;
using System.Diagnostics;
using newRBS.Database;
using System.Reflection;
using System.Windows;
using System.Xml.Serialization;
using GalaSoft.MvvmLight;


namespace newRBS.Models
{
    public enum Motor
    {
        Translation = 0,
        HorizontalTilt = 1,
        VerticalTilt = 2,
        Rotation = 3
    }

    public static class Gonio
    {
        private static string className = MethodBase.GetCurrentMethod().DeclaringType.Name;
        private static readonly Lazy<TraceSource> trace = new Lazy<TraceSource>(() => TraceSources.Create(className));

        private static TcpClient Client;
        private static Stream MessageStream;

        public static bool IsInit = false;

        public static bool Init()
        {
            string IPA = "192.168.11.2";
            int PortNR = 5000;

            System.Net.IPAddress IP = System.Net.IPAddress.Parse(IPA);
            Client = new TcpClient();

            try
            {
                Client.Connect(IP, PortNR);
                MessageStream = Client.GetStream();

                trace.Value.TraceEvent(TraceEventType.Information, 0, "Gonio opened");
                IsInit = true;
                return true;
            }
            catch (SocketException)
            {
                trace.Value.TraceEvent(TraceEventType.Error, 0, "Gonio couldn't be opened");
                IsInit = false;
                return false;
            }
        }

        public static void Close()
        {
            MessageStream.Close();
            Client.Close();

            trace.Value.TraceEvent(TraceEventType.Information, 0, "Gonio closed");
        }

        public static string WRMot(Motor? motor, string command)
        {
            if (motor != null)
                command = (char)(16 + (int)motor) + command;

            byte[] writeBuffer = command.Select(x => (byte)x).ToArray();
            
            Console.WriteLine(BitConverter.ToString(writeBuffer));
            MessageStream.Write(writeBuffer, 0, writeBuffer.Length);

            System.Threading.Thread.Sleep(100);

            byte[] readBuffer = Enumerable.Repeat((byte)0, 50).ToArray();

            int NumberOfBytes = MessageStream.Read(readBuffer, 0, readBuffer.Length);

            return Encoding.Default.GetString(readBuffer, 0, NumberOfBytes);
        }

        public static string Version()
        {
            return WRMot(null, "V");
        }

        public static string Status(Motor motor)
        {
            string Answer = WRMot(motor, ((char)7).ToString());

            if (Answer.Length == 10)
            {
                return Answer;
            }
            else
            {
                trace.Value.TraceEvent(TraceEventType.Error, 0, "Gonio isn't in PC mode");
                return "";
            }
        }

        public static double GetPosition(Motor motor)
        {
            string Answer = WRMot(motor, ((char)7).ToString());

            int position = 0;

            if (Answer.Length == 10)
            {
                position = int.Parse(string.Concat((Answer.Take(7).Reverse())));
                return position;
            }
            else
            {
                trace.Value.TraceEvent(TraceEventType.Error, 0, "Can't get motor position");
                return -1;
            }
        }

        public static void GoXSteps(Motor motor, int SW)
        {
            double absSW = Math.Abs(SW);
            if (absSW < 1048575 & absSW > 0)
            {
                string Command = "";
                byte[] SWA = new byte[5];

                SWA[4] = (byte)Math.Truncate(absSW / 16 / 16 / 16 / 16);
                absSW = absSW - SWA[4] * 16 * 16 * 16 * 16;
                SWA[3] = (byte)Math.Truncate(absSW / 16 / 16 / 16);
                absSW = absSW - SWA[3] * 16 * 16 * 16;
                SWA[2] = (byte)Math.Truncate(absSW / 16 / 16);
                absSW = absSW - SWA[2] * 16 * 16;
                SWA[1] = (byte)Math.Truncate(absSW / 16);
                SWA[0] = (byte)(absSW - SWA[1] * 16);
                Command = Command + (char)SWA[0] + (char)SWA[1] + (char)SWA[2] + (char)SWA[3] + (char)SWA[4];
                if (SW < 0) { Command = Command + (char)16; } else { Command = Command + (char)17; }
                Command = Command + (char)0 + (char)0;

                string Answer = WRMot(motor, Command);

                if (Answer[0] != 255)
                {
                    trace.Value.TraceEvent(TraceEventType.Warning, 0, "Error during 'SetStepSize', Error code: " + GetErrorCode(Answer[0]));
                }
            }
        }

        public static void Start(Motor motor)
        {
            string Answer = WRMot(motor, ((char)2).ToString());

            if (Answer[0] != 255)
            {
                trace.Value.TraceEvent(TraceEventType.Warning, 0, "Error during 'Start', Error code: " + GetErrorCode(Answer[0]));
            }
        }

        public static void Stop(Motor motor)
        {
            string Answer = WRMot(motor, ((char)3).ToString());

            if (Answer[0] != 255)
            {
                trace.Value.TraceEvent(TraceEventType.Warning, 0, "Error during 'Stop', Error code: " + GetErrorCode(Answer[0]));
            }
        }

        public static void Reset(Motor motor)
        {
            string Answer = WRMot(motor, ((char)8).ToString());

            if (Answer[0] != 255)
            {
                trace.Value.TraceEvent(TraceEventType.Warning, 0, "Error during 'Reset', Error code: " + GetErrorCode(Answer[0]));
            }
        }

        public static string GetErrorCode(char code)
        {
            switch (code)
            {
                case (char)0xE1: return "Device not in 'PC-mode'";
                case (char)0xE2: return "Command buffer full";
                case (char)0xE3: return "No data available";
                case (char)0xE4: return "Motor still driving";
                case (char)0xE5: return "Wrong data format";
                case (char)0xE6: return "Limit reached";
                case (char)0xE7: return "Unknown command";
                case (char)0xE8: return "Motor doesn't exist";
                case (char)0xE9: return "Remote control defect";
                case (char)0xEF: return "Short circuit";
                case (char)0xFF: return "Success";
                default: return "";
            }
        }
    }
}