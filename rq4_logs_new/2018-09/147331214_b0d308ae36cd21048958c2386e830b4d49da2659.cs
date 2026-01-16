using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using _Console = System.Console;

namespace ColliderGUI {
	static class Console {
		static bool ConsoleExists = false;
		static IntPtr CONIN;
		static IntPtr CONOUT;

		public static void Spawn(bool Spawn) {
			if (!Spawn)
				return;

			if (Kernel32.AllocConsole()) {
				CONIN = Kernel32.CreateFile("CONIN$", DesiredAccess.GenericRead | DesiredAccess.GenericWrite);
				CONOUT = Kernel32.CreateFile("CONOUT$", DesiredAccess.GenericWrite);

				Kernel32.SetStdHandle(StdHandle.Input, CONIN);
				Kernel32.SetStdHandle(StdHandle.Output, CONOUT);
				Kernel32.SetStdHandle(StdHandle.Error, CONOUT);
				Kernel32.SetConsoleTitle(nameof(ColliderGUI) + " Console");

				ConsoleExists = true;
				SetForeColor();
			}
		}

		public static void SetForeColor(ConsoleColor Clr = ConsoleColor.Gray) {
			if (!ConsoleExists)
				return;

			Kernel32.SetConsoleTextAttribute(CONOUT, (CharAttr)Clr);
		}

		public static void Write(string Msg) {
			if (!ConsoleExists)
				return;

			_Console.Write(Msg);
		}

		public static void Write(string Fmt, params object[] Args) {
			Write(string.Format(Fmt, Args));
		}

		public static void WriteLine(string Msg) {
			Write(Msg + "\n");
		}

		public static void WriteLine(ConsoleColor Clr, string Msg) {
			SetForeColor(Clr);
			WriteLine(Msg);
			SetForeColor();
		}

		public static void WriteLine(string Fmt, params object[] Args) {
			WriteLine(string.Format(Fmt, Args));
		}

		public static void WriteLine(ConsoleColor Clr, string Fmt, params object[] Args) {
			WriteLine(Clr, string.Format(Fmt, Args));
		}
	}
}