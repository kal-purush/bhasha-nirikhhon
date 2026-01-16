using System.IO;
using System;
using System.Net.Http;
using System.Timers;

using DSharpPlus;
using DSharpPlus.Interactivity;
using DSharpPlus.SlashCommands;
using EagleThreadBot.Common;

using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EagleThreadBot
{
	partial class Program
	{
		private static TagIndex _tagList = null;

		public static HttpClient HttpClient { get; set; } = new HttpClient();
		public static InteractivityExtension Interactivity { get; set; }
		public static TagIndex TagList
		{
			get
			{
				if(_tagList != null &&
					File.Exists("./cache/index.json") && 
					File.GetLastWriteTimeUtc("./cache/index.json") >= (DateTime.UtcNow + TimeSpan.FromMinutes(15)))
					return _tagList;

				if(!Directory.Exists("./cache"))
					Directory.CreateDirectory("./cache");
				if(!File.Exists("./cache/index.json"))
					File.Create("./cache/index.json");

				String index = HttpClient.GetStringAsync($"{Program.Configuration.TagUrl}index.json")
					.GetAwaiter().GetResult();

				// Store the index.json in cache
				File.WriteAllText("./cache/index.json", index);
				_tagList = JsonConvert.DeserializeObject<TagIndex>(index);

				return _tagList;
			}
		}
		public static Config Configuration { get; private set; }
		public static DiscordClient Client { get; private set; }
		public static SlashCommandsExtension Slashies { get; private set; }
	}
}