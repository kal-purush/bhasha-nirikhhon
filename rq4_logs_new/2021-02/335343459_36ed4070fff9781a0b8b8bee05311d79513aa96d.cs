using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using UnityEngine;
using UnityEngine.UI;

public class Token : MonoBehaviour
{
    public Text token;
    string findToken;
    
    private void Start()
    {
        var host = Dns.GetHostEntry(Dns.GetHostName());
        foreach (var ip in host.AddressList)
        {
            if (ip.AddressFamily == AddressFamily.InterNetwork)
            {
                findToken = ip.ToString();
            }
        }
       
        SetToken(findToken);
    }




    public void SetToken(string insertedToken){
        token.text= "IP: " + insertedToken;

        }
}