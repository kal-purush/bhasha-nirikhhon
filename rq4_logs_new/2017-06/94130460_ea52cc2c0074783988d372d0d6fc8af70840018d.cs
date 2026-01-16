using System.Collections;
using System.Collections.Generic;
using System;
using System.Text;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using UnityEngine;
using System.Linq;

//for more info: https://docs.microsoft.com/en-us/dotnet/framework/network-programming/asynchronous-server-socket-example

public class TCPController : MonoBehaviour
{
    public static ManualResetEvent allDone = new ManualResetEvent(false);
    //public string _IPAddress;
    private static IPAddress ip;
    // set the TcpListener on port 13000
    const int port = 13000;
    TcpListener server = new TcpListener(IPAddress.Any, port); 
    //initialize array of actuators
    public GameObject[] actuators;
    // last data recieved
    byte[] rdata;
    // data recieved
    byte[] data;
    //flag to terminate connection upon Unity exit.
    bool alive;

    private void Awake()
    {
        alive = true;
        // Start listening for client requests
        server.Start();
    }
    // Use this for initialization
    void Start()
    {
        //Sorts the Actuators of array in ascending order according to ID
        actuators = GameObject.FindGameObjectsWithTag("Actuator").OrderBy(Actuator => Actuator.GetComponent<Actuator>().ID).ToArray();
        // Sets length of data to be received to be exactly equal to number of Actuators 
        data = new byte[actuators.Length];
        // Start listening for client requests
        server.Start();
        Thread connect = new Thread(Read);
        connect.IsBackground = true;
        connect.Start();
    }

    // Update is called once per frame
    void Update()
    {
        if (data != null)
        {
                for (int i = 0; i < data.Length; i++)
                {
                    actuators[i].GetComponent<Actuator>().setState(ToBoolean(data, i));
                }

                /*foreach (GameObject actuator in actuators)
                {
                    Debug.Log(actuator.name + " : " + actuator.GetComponent<Actuator>().getState().ToString());
                }*/
        }
        
    }
    void Read()
    {
        
        // Set the event to nonsignaled state.
        allDone.Reset();
        Debug.Log("Waiting for actuator data... ");
        // Start an asynchronous socket to listen for connections.  
        server.BeginAcceptTcpClient(new AsyncCallback(AcceptCallback),server);
        // Wait until a connection is made before continuing.  
        allDone.WaitOne();        
        
    }

    private void AcceptCallback(IAsyncResult ar)
    {
        // Signal the main thread to continue.  
        allDone.Set();
        // Get the server that handles the client request.  
        TcpListener server = (TcpListener)ar.AsyncState;
        TcpClient client = server.EndAcceptTcpClient(ar);
        NetworkStream stream = client.GetStream();
        stream.BeginRead(data, 0, data.Length,new AsyncCallback(ReadCallback), stream);
    }

    private void ReadCallback(IAsyncResult ar)
    {
        // Retrieve the server's networkstream
        NetworkStream stream = (NetworkStream)ar.AsyncState;
        // Read data from the client. 
        stream.EndRead(ar);
        //Loop again as long as Unity is active
        if (alive)
            Read();
        else stream.Close();
    }
    // Process the data received and changing it to boolean variable
    public bool ToBoolean(byte[] data, int i) { return Convert.ToBoolean((data[i] & 0x1)); }
    

    void OnApplicationQuit()
    {
        alive = false;
        server.Stop();        
    }

}