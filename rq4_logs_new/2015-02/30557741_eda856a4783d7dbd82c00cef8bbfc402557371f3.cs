using UnityEngine;
using System;
using System.Collections;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.IO;

public class PlayerNetCommunicate : MonoBehaviour
{
	
	TcpListener tcpListenerLeft;
	TcpListener tcpListenerRight;
	Boolean left;
	Boolean right;
	int PORT = 4444;

	void Start()
	{
		//Debug.Log (LocalIPAddress());
		InitializeListener ();
	}

	void InitializeListener() {
		tcpListenerLeft = CreateListener (PORT);
		tcpListenerLeft.Start();
		
		tcpListenerRight = CreateListener (PORT);
		tcpListenerRight.Start();
		
		StartToListen ();
	}

	TcpListener CreateListener(int port) {
		TcpListener listen = new TcpListener (IPAddress.Parse (LocalIPAddress ()), port);
		PORT++;
		return listen;
	}

	void StartToListen() {

		UnityThreadHelper.CreateThread(() =>
		                               {
			NetworkStream streamLeft = tcpListenerLeft.AcceptTcpClient().GetStream();
			Debug.Log("Left Connection accepted.");
			
			StreamReader streamReaderLeft = new StreamReader(streamLeft);
			left = true;
			HandOversStreamReader();
			
		});
		
		UnityThreadHelper.CreateThread(() =>
		                               {
			NetworkStream streamRight = tcpListenerRight.AcceptTcpClient().GetStream();
			Debug.Log("Right Connection accepted.");
			
			StreamReader streamReaderRight = new StreamReader(streamRight);
			right = true;
			HandOversStreamReader();

		});

	}
	
	void HandOversStreamReader () {

		if (left && right) {
			//Hand over StreamReader to Player
			right = false;
			left = false;
			InitializeListener ();
		}

	}
	
	public string LocalIPAddress()
	{
		IPHostEntry host;
		string localIP = "";
		host = Dns.GetHostEntry(Dns.GetHostName());
		foreach (IPAddress ip in host.AddressList)
		{
			if (ip.AddressFamily == AddressFamily.InterNetwork)
			{
				localIP = ip.ToString();
				break;
			}
		}
		return localIP;
	}

}


//int angleLeft;
//int distanceLeft;
//int angleRight;

/**while (true) {
				
				String data = streamReaderLeft.ReadLine();
				String[] tokens = data.Split(' ');
				
				if (tokens.Length == 2) {
					if (tokens[0] != "") {
						angleLeft = int.Parse(tokens[0].Substring(1));
					}
					
					if (tokens[1] != "") {
						distanceLeft = int.Parse(tokens[1].Substring(1));
					}
					
				} else if (tokens.Length == 1) {
					
					if (tokens[0] != "") {
						if (tokens[0].Substring(0,1).Equals("0")) {
							angleLeft = int.Parse(tokens[0].Substring(1));
						}
						
						if (tokens[0].Substring(0,1).Equals("1")) {
							distanceLeft = int.Parse(tokens[0].Substring(1));
						}
					}
				}
				
			};**/

/**while (true) {
				
				String data = streamReaderRight.ReadLine();
				
				if (data.Substring(0,1).Equals("a")) {
					
				} else {
					angleRight = int.Parse(data);
				}
				
			};
			**/

/**gameObject.transform.rotation = Quaternion.AngleAxis(angleRight, Vector3.up);
		
		Vector3 newPosition = gameObject.transform.position;
		
		newPosition.x += (float)(Math.Cos (DegreeToRadian (angleLeft)) * distanceLeft * 0.001);
		newPosition.z += (float)(Math.Sin (DegreeToRadian (angleLeft)) * distanceLeft * 0.001);
		
		gameObject.transform.position = newPosition;**/

/** private double DegreeToRadian(int angle) {
		return (270.0 - (Math.PI * (double)angle / 180.0));
	}**/

//string responseString = "You have successfully connected to me";

//Forms and sends a response string to the connected client.
//Byte[] sendBytes = Encoding.ASCII.GetBytes(responseString);
//stream.Write(sendBytes, 0, sendBytes.Length);
//Debug.Log("Message Sent /> : " + responseString);