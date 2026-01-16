using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Sim_Harness_GUI
{
public class JsonFile
{
	private Dictionary<int, JsonUser> _users;
	private Dictionary<int, JsonHouse> _houses;
	private bool _error;
	public JsonFile(String jsonConfig)
	{
		_error = jsonStringParser(jsonConfig)
	}

	private bool jsonStringParser(string jsonConfig)
	{
		if(String.IsNullOrEmpty(jsonConfig))
		{
			return false;
		}

		JObject info = null;


		try
		{
			info = JObject.Parse(jsonConfig);
		}
		catch(JsonException ex)
		{
			var error = String.Format("Scenario parsing error: {0}", ex.Message);
			Console.WriteLine(error);
			return false;
		}

		JToken house_list;
		JToken user_list;

		// Get all houses
		if(!info.TryGetValue("houses", out house_list))
		{
			_houses = new Dictionary<int, JsonHouse>();
		}
		else
		{
			IJEnumerable<JToken> houses = house_list.Children();

			foreach(JToken house in houses)
			{
				JsonHouse newHouse = new JsonHouse(house);
				_houses.Add(newHouse.id, newHouse);
			}
		}

		// Get all users
		if(!info.TryGetValue("users", out user_list))
		{
			_users = new Dictionary<int, JsonHouse>();
		}
		else
		{
			IJEnumerable<JToken> users = user_list.Children();

			foreach(JToken user in users)
			{
				JsonHouse newUser = new JsonUser(user);
				_houses.Add(newUser.id(), newUser);
			}
		}



		return true;

		bool status = false;


		//search through houses. Pity this isn't a map.
		foreach(JToken house in houses)
		{
			JObject house_obj = JObject.Parse(house.ToString());
			JToken name;

			//found our house
			if(house_obj.TryGetValue("name", out name) &&
				name.ToString() == house_id)
			{
				JToken port_tok;
				JToken dev_tok;
				if(house_obj.TryGetValue("port", out port_tok))
				{
					_port = JsonConvert.DeserializeObject<int>(port_tok.ToString());
					//must get a valid port value
					if(_port > System.Net.IPEndPoint.MaxPort || _port < System.Net.IPEndPoint.MinPort)
					{
						return false;
					}
				}
				bool success = house_obj.TryGetValue("devices", out dev_tok);
				System.Diagnostics.Debug.Assert(success);
				IJEnumerable<JToken> devices = dev_tok.Children();
				UInt64 id = 0;
				foreach(JToken dev in devices)
				{
					//TODO: Create DeviceInput and DeviceOutput for control
					Device device = Interfaces.DeserializeDevice(dev.ToString(), null, null, new TimeFrame());

					if(device != null)
					{
						device.ID.DeviceID = id++;
						DeviceModel.Instance.Devices.Add(device);
					}
				}

				JToken weather_tok;
				success = house_obj.TryGetValue("weather", out weather_tok);
				System.Diagnostics.Debug.Assert(success);
				_weather = new LinearWeather();
				IJEnumerable<JToken> temps = weather_tok.Children();
				foreach(JToken temp in temps)
				{
					_weather.Add(JsonConvert.DeserializeObject<TemperatureSetPoint>(temp.ToString()));
				}

				System.Diagnostics.Debug.Assert(DeviceModel.Instance.Devices.Count > 0);
				status = true;
				break;
			}
		}
		return true;
	}


}//class
}//namespace
