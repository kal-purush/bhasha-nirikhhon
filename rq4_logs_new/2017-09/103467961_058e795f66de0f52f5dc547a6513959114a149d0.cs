using System;
using System.Security.Cryptography;

namespace Business
{
    public class Session
    {
        public Session(Client client)
        {
            Id = GenerateRandomToken();
            Client = client;
            ConnectedSince = DateTime.Now;
            Active = true;
        }

        public string Id { get; private set; }
        public Client Client { get; private set; }
        public DateTime ConnectedSince { get; set; }
        public DateTime ConnectedTo { get; set; }
        public bool Active { get; set; }

        public void Deactivate()
        {
            ConnectedTo = DateTime.Now;
            Active = false;
        }
        
        private static string GenerateRandomToken(int length = 12)
        {
            var cryptRng = new RNGCryptoServiceProvider();
            var tokenBuffer = new byte[length];
            cryptRng.GetBytes(tokenBuffer);
            return Convert.ToBase64String(tokenBuffer);
        }
    }
}