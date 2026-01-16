using System;
using ImdbNet.Core;
using ImdbNet.Core.Models;
using NUnit.Framework;


namespace ImdbNet.Tests
{
	[TestFixture]
	public class NamesTests
	{
		[Test]
		public void TestGetPerson()
		{
			//var rng = new Random(DateTime.Now.TimeOfDay.Milliseconds);
			//var ind = rng.Next(Top50Names.Length);
			//var name = Top50Names[ind];
			//var person = Utils.GetPerson(name.id);
			var person = Utils.GetPerson("nm0000209");
		}

		private static (string id, string url, string name)[] Top50Names =
		{
			("nm0597410", "https://www.imdb.com/name/nm0597410", "Taylor Momsen"),
			("nm0488953", "https://www.imdb.com/name/nm0488953", "Brie Larson"),
			("nm0597388", "https://www.imdb.com/name/nm0597388", "Jason Momoa"),
			("nm1231899", "https://www.imdb.com/name/nm1231899", "Priyanka Chopra"),
			("nm6406427", "https://www.imdb.com/name/nm6406427", "Thea Sofie Loch Næss"),
			("nm4101853", "https://www.imdb.com/name/nm4101853", "Alexander Dreymon"),
			("nm0272581", "https://www.imdb.com/name/nm0272581", "Rebecca Ferguson"),
			("nm3014031", "https://www.imdb.com/name/nm3014031", "Rachel Brosnahan"),
			("nm0082526", "https://www.imdb.com/name/nm0082526", "Peter Billingsley"),
			("nm1785339", "https://www.imdb.com/name/nm1785339", "Rami Malek"),
			("nm1720028", "https://www.imdb.com/name/nm1720028", "Amber Heard"),
			("nm3053338", "https://www.imdb.com/name/nm3053338", "Margot Robbie"),
			("nm2368789", "https://www.imdb.com/name/nm2368789", "Zoë Kravitz"),
			("nm1564087", "https://www.imdb.com/name/nm1564087", "Jenna Dewan"),
			("nm3948952", "https://www.imdb.com/name/nm3948952", "Vanessa Kirby"),
			("nm0362766", "https://www.imdb.com/name/nm0362766", "Tom Hardy"),
			("nm2215143", "https://www.imdb.com/name/nm2215143", "Kiernan Shipka"),
			("nm0935395", "https://www.imdb.com/name/nm0935395", "Katheryn Winnick"),
			("nm2110418", "https://www.imdb.com/name/nm2110418", "Gemma Chan"),
			("nm1935086", "https://www.imdb.com/name/nm1935086", "Tessa Thompson"),
			("nm2794962", "https://www.imdb.com/name/nm2794962", "Hailee Steinfeld"),
			("nm2079733", "https://www.imdb.com/name/nm2079733", "Daniela Ruah"),
			("nm6073955", "https://www.imdb.com/name/nm6073955", "Florence Pugh"),
			("nm0664175", "https://www.imdb.com/name/nm0664175", "Jessica Paré"),
			("nm0115161", "https://www.imdb.com/name/nm0115161", "Emily Browning"),
			("nm1086543", "https://www.imdb.com/name/nm1086543", "Amanda Seyfried"),
			("nm3918035", "https://www.imdb.com/name/nm3918035", "Zendaya"),
			("nm0000673", "https://www.imdb.com/name/nm0000673", "Marisa Tomei"),
			("nm2394794", "https://www.imdb.com/name/nm2394794", "Karen Gillan"),
			("nm0000621", "https://www.imdb.com/name/nm0000621", "Kurt Russell"),
			("nm0577982", "https://www.imdb.com/name/nm0577982", "Harry Melling"),
			("nm1358539", "https://www.imdb.com/name/nm1358539", "Jennifer Carpenter"),
			("nm0005508", "https://www.imdb.com/name/nm0005508", "Janine Turner"),
			("nm2539953", "https://www.imdb.com/name/nm2539953", "Alicia Vikander"),
			("nm0005154", "https://www.imdb.com/name/nm0005154", "Lucy Liu"),
			("nm2946516", "https://www.imdb.com/name/nm2946516", "Claire Foy"),
			("nm0931329", "https://www.imdb.com/name/nm0931329", "Michelle Williams"),
			("nm1275259", "https://www.imdb.com/name/nm1275259", "Alexandra Daddario"),
			("nm0009918", "https://www.imdb.com/name/nm0009918", "Amy Acker"),
			("nm0570860", "https://www.imdb.com/name/nm0570860", "Rose McIver"),
			("nm1519680", "https://www.imdb.com/name/nm1519680", "Saoirse Ronan"),
			("nm0001303", "https://www.imdb.com/name/nm0001303", "Carla Gugino"),
			("nm1289434", "https://www.imdb.com/name/nm1289434", "Emily Blunt"),
			("nm1423048", "https://www.imdb.com/name/nm1423048", "Sarah Rafferty"),
			("nm1745736", "https://www.imdb.com/name/nm1745736", "Paula Patton"),
			("nm1379938", "https://www.imdb.com/name/nm1379938", "Travis Fimmel"),
			("nm1157048", "https://www.imdb.com/name/nm1157048", "Zachary Levi"),
			("nm2640887", "https://www.imdb.com/name/nm2640887", "Stefanie Scott"),
			("nm0000098", "https://www.imdb.com/name/nm0000098", "Jennifer Aniston"),
			("nm3592338", "https://www.imdb.com/name/nm3592338", "Emilia Clarke")
		};
	}
}