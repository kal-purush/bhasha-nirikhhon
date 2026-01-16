using System;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace OMineGuard.Backend
{
    public static class InternetConnectionWacher
    {
        static InternetConnectionWacher()
        {
            StartWachInternetConnection();
        }

        public static event Action InternetConnectionLost;
        public static event Action InternetConnectionRestored;
        public static bool InternetConnectedState { get; private set; } = true;

        private static int WachDelay = 3; //sec
        private static void StartWachInternetConnection()
        {
            Task.Run(async () => 
            {
                bool ICSnew;
                while (true)
                {
                    ICSnew = InternetConnetction();
                    if (InternetConnectedState != ICSnew)
                    {
                        if (ICSnew)
                        {
                            InternetConnectionRestored.Invoke();
                        }
                        else
                        {
                            InternetConnectionLost.Invoke();
                        }
                        InternetConnectedState = ICSnew;
                    }
                    await Task.Delay(WachDelay * 1000);
                }
            });
        }

        private static bool InternetConnetction()
        {
            InternetConnectionState_e cs = new InternetConnectionState_e();
            InternetGetConnectedState(ref cs, 0);

            bool[] IC = new bool[]
            {
                (cs & InternetConnectionState_e.INTERNET_CONNECTION_LAN) == InternetConnectionState_e.INTERNET_CONNECTION_LAN,
                (cs & InternetConnectionState_e.INTERNET_CONNECTION_MODEM) == InternetConnectionState_e.INTERNET_CONNECTION_MODEM,
                (cs & InternetConnectionState_e.INTERNET_CONNECTION_PROXY) == InternetConnectionState_e.INTERNET_CONNECTION_PROXY
            };
            IPStatus? status = null;
            if (IC[0] || IC[1] || IC[2])
            {
                Ping ping = new Ping();
                for (byte i = 0; i < 4; i++)
                {
                    try { status = ping.Send("8.8.8.8").Status; }
                    catch { }
                    if (status == IPStatus.Success) return true;
                }
            }
            return false;
        }
        /// DLLimport
        [DllImport("wininet.dll", CharSet = CharSet.Auto)]
        private extern static bool InternetGetConnectedState(ref InternetConnectionState_e lpdwFlags, int dwReserved);
        [Flags]
        private enum InternetConnectionState_e : int
        {
            INTERNET_CONNECTION_MODEM = 0x01,      // true
            INTERNET_CONNECTION_LAN = 0x02,     // true 
            INTERNET_CONNECTION_PROXY = 0x04,       // true
            INTERNET_RAS_INSTALLED = 0x10,
            INTERNET_CONNECTION_OFFLINE = 0x20,
            INTERNET_CONNECTION_CONFIGURED = 0x40
        }
    }
}