using System;
using System.Net.Http;
using System.Diagnostics;
using System.Text;

namespace HomeAutomationApp
{
public static class AddRoomController
{

	public static HttpResponseMessage SendRoomAsync(string packet)
	{
		try
		{
			var client = new HttpClient();
			client.Timeout = TimeSpan.FromSeconds(10);


			var response = client.PostAsync("http://serverapi1.azurewebsites.net/api/storage/space/", 
				new StringContent(packet, Encoding.UTF8, "application/json")).Result;

			return response;
		}

		catch(Exception e)
		{
			Debug.WriteLine("HAD - Position Update Error: " + e.Message);
			Debug.WriteLine("HAD - Position Update Error: " + e.InnerException.Message);
		}

		return null;
	}
}
}
