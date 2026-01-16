using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FishGfx;

namespace ColliderGUI {
	public static unsafe class LegClient {
		const bool FakePosition = false;

		static UdpClient UDP;
		static int Port = 40024;

		public static Vector3 L_Start;
		public static Vector3 L_End;

		public static Vector3 R_Start;
		public static Vector3 R_End;

		public static void Init(int Port) {
			LegClient.Port = Port;

			Thread WorkerThread = new Thread(() => {
				if (!FakePosition) {
					UDP = new UdpClient(Port);
					UDP.DontFragment = true;
				}

				while (true)
					ReceiveVectors();
			});
			WorkerThread.IsBackground = true;
			WorkerThread.Start();
		}

		public static byte[] ReceiveRaw() {
			IPEndPoint Sender = new IPEndPoint(IPAddress.Any, Port);
			return UDP.Receive(ref Sender);
		}

		static float GetFloat(byte[] Bytes, int Idx) {
			return (float)BitConverter.ToDouble(Bytes, Idx * sizeof(double));
		}

		public static void ReceiveVectors() {
			byte[] Bytes = ReceiveRaw();

			R_Start = Transform(new Vector3(GetFloat(Bytes, 0), GetFloat(Bytes, 1), GetFloat(Bytes, 2)));
			R_End = Transform(new Vector3(GetFloat(Bytes, 3), GetFloat(Bytes, 4), GetFloat(Bytes, 5)));
		}

		static Vector3 Transform(Vector3 In) {
			return In.YZX() + OptotrakClient.GetPos() - Program.CameraHeight;
		}
	}
}