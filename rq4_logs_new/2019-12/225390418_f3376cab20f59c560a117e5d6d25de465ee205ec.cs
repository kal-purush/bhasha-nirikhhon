using Common;
using System;
using System.ServiceModel;

namespace Client
{
    public class ClientProxy : ChannelFactory<IPrimaryService>
    {
        private IPrimaryService factory;

        public ClientProxy(NetTcpBinding binding, EndpointAddress remoteAddress) : base(binding, remoteAddress)
        {
            // TODO: podesiti autentifikaciju sa PrimaryService
            // TODO: podesiti autorizaciju sa PrimaryService
            factory = CreateChannel();
        }

        public void SendAlarm(Alarm alarm)
        {
            try
            {
                factory.SendAlarm(alarm);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e.Message);
            }
        }
    }
}