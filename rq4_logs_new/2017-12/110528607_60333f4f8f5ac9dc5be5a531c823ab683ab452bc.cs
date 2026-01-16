using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

using Newtonsoft.Json;
using RestSharp;
using System.Collections.Generic;

namespace CRM_GTMK.Model
{


	public class ApiClient
	{
		private const string accountId = "";
		private const string ApiKey = "";

		public static string ProjectsListUrl { get; private set; } = "https://smartcat.ai/api/integration/v1/project/list";

		public static string AuthorizationValue { get; set; }

		private enum HttpMethods
		{
			[Description("GET")]
			Get,
			[Description("POST")]
			Post,
			[Description("PUT")]
			Put,
			[Description("DELETE")]
			Delete
		}

		public ApiClient()
		{
			AuthorizationValue = accountId + ":" + ApiKey;
		}

		public List<Project> GetCurrentProjects()
		{
			var projects = new List<Project>();

			HttpWebRequest request = createRequest(ProjectsListUrl, HttpMethods.Get);

			HttpWebResponse response = request.GetResponse() as HttpWebResponse;

			using (Stream stream = response.GetResponseStream())
			using (var reader = new StreamReader(stream))

			{
				var text = reader.ReadToEnd();
				projects = JsonConvert.DeserializeObject<List<Project>>(text);
			}

			return projects;
		}

		private HttpWebRequest createRequest(string url, HttpMethods method)
		{
			var authorizationValueBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(AuthorizationValue));
			var request = (HttpWebRequest)WebRequest.Create(url);

			request.Headers.Add("Authorization", "Basic " + authorizationValueBase64);
			request.Method = method.Description();
			request.ContentType = "text/json";
			request.Timeout = 200000;

			return request;
		}

	}

	public static class EnumExtensions
	{
		public static string Description(this Enum value)
		{
			var descriptionAttribute = (DescriptionAttribute[])(value.GetType().GetField(value.ToString())).GetCustomAttributes(typeof(DescriptionAttribute), false);
			return descriptionAttribute.Length > 0 ? descriptionAttribute[0].Description : value.ToString();
		}

		public static T GetValueFromDescription<T>(string description)
		{
			var type = typeof(T);

			if (!type.IsEnum)
			{
				throw new Exception(String.Format("Тип {0} не является перечислением.", type));
			}

			foreach (var field in type.GetFields())
			{
				var attribute = Attribute.GetCustomAttribute(field, typeof(DescriptionAttribute)) as DescriptionAttribute;
				if (attribute != null)
				{
					if (attribute.Description == description)
					{
						return (T)field.GetValue(null);
					}
				}
				else
				{
					if (field.Name == description)
					{
						return (T)field.GetValue(null);
					}
				}
			}

			throw new Exception(String.Format("Не удалось найти значение перечисления {0} по значению атрибута Description:{1}", type, description));
		}
	}
}


