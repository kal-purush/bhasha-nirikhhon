using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EasyGelf.LoadTest
{
	class Program
	{
		private static readonly ILog Log = LogManager.GetLogger("ExampleLog");

		static void Main(string[] args)
		{
			int howManyThreads = System.Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["threadsCount"]);

			var fileInfo = new FileInfo("log4net.config");
			if (!fileInfo.Exists)
				throw new Exception();
			XmlConfigurator.Configure(fileInfo);

			for (int i = 0; i < howManyThreads; i++)
			{
				var threadTestLogNet = new Thread(new ThreadStart(TestLogNet));
				threadTestLogNet.IsBackground = true;
				threadTestLogNet.Start();

				var threadTestIISLog = new Thread(new ThreadStart(TestIISLog));
				threadTestIISLog.IsBackground = true;
				threadTestIISLog.Start();

				var threadTestWindowsEvents = new Thread(new ThreadStart(TestWindowsEvents));
				threadTestWindowsEvents.IsBackground = true;
				threadTestWindowsEvents.Start();
			}

			Console.ReadLine();
		}

		private static void TestLogNet()
		{
			int millisecondsTimeout = System.Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["millisecondsTimeout"]);
			int count = System.Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["countToSend"]);
			string testID = System.Configuration.ConfigurationManager.AppSettings["TestID"];
			string errorText = "empty error";
			try
			{
				FooLevel1();
			}
			catch (Exception exc)
			{
				errorText = exc.ToString();
			}

			for (int i = 0; i < count; i++)
			{
				Log.Info(String.Format("Log4Net load test message i={3}. TestID:{4}. Computername:'{0}', ManagedThreadId:'{1}', computer date time:'{2}'. Error is:{5}", Environment.MachineName, System.Threading.Thread.CurrentThread.ManagedThreadId, DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"), i, testID, errorText));

				System.Threading.Thread.Sleep(millisecondsTimeout);
			}
		}

		private static void TestIISLog()
		{
			int millisecondsTimeout = System.Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["millisecondsTimeout"]);

			while (true)
			{
				//WebClient webClient = new WebClient();
				//string result = webClient.Dow("http://localhost");

				System.Threading.Thread.Sleep(millisecondsTimeout);
			}

		}

		private static void TestWindowsEvents()
		{
			int millisecondsTimeout = System.Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["millisecondsTimeout"]);
			int count = System.Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["countToSend"]);
			string testID = System.Configuration.ConfigurationManager.AppSettings["TestID"];
			string errorText = "empty error";

			try
			{
				FooLevel1();
			}
			catch (Exception exc)
			{
				errorText = exc.ToString();
			}

			for (int i = 0; i < count; i++)
			{
				System.Diagnostics.EventLog appLog = new System.Diagnostics.EventLog();
				appLog.Source = "Load test event";
				appLog.WriteEntry(String.Format("Windows event load test message i={3}. TestID:{4}. Computername:'{0}', ManagedThreadId:'{1}', computer date time:'{2}'. Error is:{5}", Environment.MachineName, System.Threading.Thread.CurrentThread.ManagedThreadId, DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt"), i, testID, errorText));

				System.Threading.Thread.Sleep(millisecondsTimeout);
			}
		}

		private static void FooLevel1()
		{
			FooLevel2();
		}
		private static void FooLevel2()
		{
			FooLevel3();
		}

		private static void FooLevel3()
		{
			FooLevel4();
		}

		private static void FooLevel4()
		{
			FooLevel5();
		}

		private static void FooLevel5()
		{
			FooLevel6();
		}

		private static void FooLevel6()
		{
			throw new Exception("Test error occurred here - in method FooLevel6.");
		}

	}
}