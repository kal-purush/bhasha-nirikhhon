using Newtonsoft.Json;
using System;
using System.Net;
using System.Text;

namespace ConsoleApplication2
{
	class Program
	{
		public static void Main()
		{
			WebClient webClient = new WebClient();

			webClient.Encoding = Encoding.UTF8;

			webClient.Headers.Add("user-agent", "Mozilla/4.0");

			webClient.QueryString.Add("specialization", "1.221");
			webClient.QueryString.Add("area", "72");

			string response = webClient.DownloadString("https://api.hh.ru/vacancies");

			dynamic list = JsonConvert.DeserializeObject(response);

			foreach (var item in list.items)
			{
				Console.WriteLine(item.name);
				Console.WriteLine(item.snippet.requirement);
				Console.WriteLine(item.snippet.responsibility);
				Console.WriteLine();
			}
			
			Console.ReadLine();
		}
	}
}