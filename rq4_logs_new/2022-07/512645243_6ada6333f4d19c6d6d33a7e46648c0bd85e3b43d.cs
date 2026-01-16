using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using EPS.Api;
using Unity.Netcode;

namespace EPS
{
    public enum NetworkSytemType { UNDEFINED, HOST, CLIENT, SERVER };

    public class Networking : EPS.INetworking, EPS.Foundation.IService
    {
        private NetWorkManager networkManager = null;
        private NetworkSytemType currentSystemType;

        public Networking()
        {
            networkManager = NetworkManager.Singleton;
        }
        
        //INetworking
        public void Start(NetworkSytemType type, string ip, int port)
        {
            //Configure netcode
            
            //Init
            switch (type)
            {
                case NetworkSytemType::HOST:
                    return networkManager.StartHost();
                case NetworkSytemType::SERVER:
                    return networkManager.StartServer();
                case NetworkSytemType::CLIENT:
                    networkManager.GetComponent<UNetTransport>().ConnectAddress = ip; //takes string
                    networkManager.GetComponent<UNetTransport>().ConnectPort = port;  //takes integer
                    return networkManager.StartClient();
                break;    
            }
            currentSystemType = type;
        }

        public void Stop()
        {
            switch (currentSystemType)
            {
                case NetworkSytemType::HOST:
                    return networkManager.StopHost();
                case NetworkSytemType::SERVER:
                    return networkManager.StopServer();
                case NetworkSytemType::CLIENT:
                    return networkManager.StopClient();
                break;    
            }
        }

        //IService
        public System.Func<IEnumerator> LoopCourrutine()
        {
            return null;
        }

        public void Loop()
        {
        }


        public void OnDestroy()
        {
        }

        public void OnInit(DependencyManager manager)
        {
        }

        public void OnReset()
        {
        }

        public string ReferencedName()
        {
            return this.GetType().ToString();
        }
    }
}
