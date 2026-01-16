using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Net;
using System.Net.NetworkInformation;

public class NetworkUtils
{
    public static string GetLocalIPAddress()
    {
        string localIP = "Not Connected";
        foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (networkInterface.OperationalStatus == OperationalStatus.Up)
            {
                var ipProperties = networkInterface.GetIPProperties();
                foreach (var address in ipProperties.UnicastAddresses)
                {
                    if (address.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && !IPAddress.IsLoopback(address.Address))
                    {
                        localIP = address.Address.ToString();
                        break;
                    }
                }
            }
        }
        return localIP;
    }
}