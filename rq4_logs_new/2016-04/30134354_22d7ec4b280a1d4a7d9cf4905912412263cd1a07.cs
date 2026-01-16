using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace LogArchive.Models
{
	/// <summary>
	/// ID,날짜,해역이름,해역,적 함대,랭크,드랍
	/// </summary>
	public class DropStringLists
	{
		public int Id { get; set; }
		public string Date { get; set; }
		public string SeaArea { get; set; }
		public string MapInfo { get; set; }
		public string EnemyFleet { get; set; }
		public string Rank { get; set; }
		public string Drop { get; set; }

		public void CopyBattleData(int id)
		{
			string MainFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);

			if (File.Exists(Path.Combine(MainFolder, "replaydata.json")))
			{
				JObject json = JObject.Parse(File.ReadAllText(Path.Combine(MainFolder, "replaydata.json")));

				Clipboard.SetText(JsonConvert.SerializeObject((JObject)json[id.ToString()]));
			}
		}
	}
	/// <summary>
	/// 날짜,결과,비서함,연료,탄,강재,보크사이트
	/// </summary>
	public class ItemStringLists
	{
		public string Date { get; set; }
		public string Results { get; set; }
		public string Assistant { get; set; }
		public int Fuel { get; set; }
		public int Steel { get; set; }
		public int Bullet { get; set; }
		public int bauxite { get; set; }
	}
	/// <summary>
	/// 날짜,결과,연료,탄,강재,보크사이트,개발자재
	/// </summary>
	public class BuildStirngLists
	{
		public string Date { get; set; }
		public string Results { get; set; }
		public int Fuel { get; set; }
		public int Steel { get; set; }
		public int Bullet { get; set; }
		public int bauxite { get; set; }
		public int UseItems { get; set; }
	}
}