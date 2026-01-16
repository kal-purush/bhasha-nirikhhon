using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PicartoWrapperAPI.Clients
{
    public class PicartoFixedClient : PicartoAuthenticatedClient, IPicartoClient
    {
        public PicartoFixedClient(string clientId, string token) : base(clientId)
        {
            restClient.AddDefaultHeader("Authorization", String.Format("Bearer {0}", token));
        }
    }
}