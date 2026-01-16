using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using UnityEngine;

public class AConnection : MonoBehaviour
{
	public int _ReceivePortNumber;
	public string _MulticastIP;

	private UdpClient rec;
	private IPAddress multicastaddress;
	private IPEndPoint remoteep;

	Thread recieveTester;
	//flag to terminate thread upon game exit
	bool alive;
	//state representing the current output of the actuator.
	bool state;

	void Awake ()
	{ 
		rec = new UdpClient (_ReceivePortNumber);
		multicastaddress = IPAddress.Parse (_MulticastIP);
		alive = true;
		    
    
	}

	void Start ()
	{
        
		rec.JoinMulticastGroup (multicastaddress);
		recieveTester = new Thread (new ThreadStart (ReceiveData));
		recieveTester.IsBackground = true;
		recieveTester.Start ();

	}

	void Update ()
	{
	}

	public void ReceiveData ()
	{
		
		while (alive) {
			byte[] data = rec.Receive (ref remoteep);
			state = ToBoolean (data);
			Debug.Log (state.ToString ()); // Unessential command. Used for tracing purpose.
			Thread.Sleep (30);
		}

	}

    // Process the data received and changing it to boolean variable
	public bool ToBoolean (byte[] data)
	{
		return Convert.ToBoolean ((data [0] & 0x1));

	}

    //Terminating the UDP connection upon exit from game.
	void OnApplicationQuit ()
	{
		alive = false;
	}
}