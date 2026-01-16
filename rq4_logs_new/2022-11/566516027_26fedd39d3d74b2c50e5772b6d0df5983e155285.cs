using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.InteropServices;
using System.Diagnostics;

namespace PacketTracerHack
{
    class Program
    {
		public static IntPtr packetTracer;
		public static string process = "PacketTracer";
		public static VAMemory vam = new VAMemory(process);
		static void Main(string[] args)
		{
			string newName;
			Program prog = new Program();
			packetTracer = prog.GetModule("PacketTracer.exe");
			Console.WriteLine(vam.ReadInt32((IntPtr)0x7ff7d5e27348));
			Console.WriteLine("------");
			Console.WriteLine("Staré meno: " + vam.ReadStringUnicode(prog.PointerPath(new long[] { (long)packetTracer + 0x03F97348, 0x320, 0xC0, 0x38 }) + 0x18, 16));
			Console.WriteLine("Nove meno: ");
			newName = Console.ReadLine();
			vam.WriteStringUnicode(prog.PointerPath(new long[] { (long)packetTracer + 0x03F97348, 0x320, 0xC0, 0x38 }) + 0x18, newName.PadRight(8, (char)0));
			Console.WriteLine("OK");
			Console.ReadLine();
		}
		IntPtr GetModule(string moduledll)
		{
			try
			{
				Process[] p = Process.GetProcessesByName(process);
				if (p.Length > 0)
				{
					foreach (ProcessModule m in p[0].Modules)
						if (m.ModuleName == moduledll)
							return m.BaseAddress;
					return (IntPtr)0;
				}
				else
					return (IntPtr)0;
			}
			catch (Exception ex)
			{
				return (IntPtr)0;
			}
		}
		IntPtr PointerPath(long[] path)
		{
			long temp=0;
			for (int i = 0; i < path.Length; i++)
			{
				Console.WriteLine(temp.ToString("x"));
				Console.WriteLine(path[i].ToString("x"));
				temp = vam.ReadLong((IntPtr)(temp + path[i]));
			}
			return (IntPtr)temp;
		}
	}
}