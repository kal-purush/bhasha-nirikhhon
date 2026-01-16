using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using UnityEngine;
using System.Linq;

//for more info: https://docs.microsoft.com/en-us/dotnet/framework/network-programming/asynchronous-server-socket-example

public class TCPOneServer : MonoBehaviour
{
    public static ManualResetEvent allDone = new ManualResetEvent(false);
    //public string _IPAddress;
    private static IPAddress ip;
    // set the TcpListener on port 13000
    const int port = 13000;
    TcpListener server = new TcpListener(IPAddress.Any, port);
    //initialize array of actuators/sensors
    public GameObject[] actuators;
    public GameObject[] sensors;
    // data recieved
    byte[] data;
    // data to be sent. A single byte for each boolean value.
    byte[] sdata;
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
        //Sorts the Actuators/Sensors of array in ascending order according to ID
        actuators = GameObject.FindGameObjectsWithTag("Actuator").OrderBy(Actuator => Actuator.GetComponent<Actuator>().ID).ToArray();
        sensors = GameObject.FindGameObjectsWithTag("Sensor").OrderBy(sensor => sensor.GetComponent<Sensor>().ID).ToArray();
        // Sets length of data to be received to be exactly equal to number of Actuators 
        data = new byte[actuators.Length];
        sdata = new byte[sensors.Length];
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
            foreach (GameObject sensor in sensors)
            {
                sdata[sensor.GetComponent<Sensor>().ID] = To4DIACBoolean(sensor.GetComponent<Sensor>().getState());
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
        server.BeginAcceptTcpClient(new AsyncCallback(AcceptCallback), server);
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
        stream.BeginRead(data, 0, data.Length, new AsyncCallback(ReadCallback), stream);
    }

    private void ReadCallback(IAsyncResult ar)
    {
        Thread.Sleep(2000);
        // Retrieve the server's networkstream
        NetworkStream stream = (NetworkStream)ar.AsyncState;
        // Read actuator data from the client. 
        stream.EndRead(ar);
        Debug.Log(System.Text.Encoding.ASCII.GetString(data));
        // Send sensor data to the stream.
        if (stream.CanWrite)
            stream.Write(sdata, 0, sdata.Length);
        //Loop again as long as Unity is active
        if (alive)
            stream.BeginRead(data, 0, data.Length, new AsyncCallback(ReadCallback), stream);
        else stream.Close();
    }
    // Process the data received and changing it to boolean variable
    public bool ToBoolean(byte[] data, int i) { return Convert.ToBoolean((data[i] & 0x1)); }
    byte To4DIACBoolean(bool state)
    {
        if (state)
            return 0x41;
        else
            return 0x40;
    }

    void OnApplicationQuit()
    {
        alive = false;
        server.Stop();
    }

}