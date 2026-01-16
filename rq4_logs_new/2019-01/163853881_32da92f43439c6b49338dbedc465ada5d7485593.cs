using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MusicPlayer.Extensions
{
	static class SongExtensions
	{
		public static List<Song> Shuffle(this List<Song> srcList)
		{
			var tmpList = new List<Song>();

			var rand = new Random();
			int randSongNum, cntr = srcList.Count;

			for (int i = 0; i < cntr; i++)
			{
				randSongNum = rand.Next(srcList.Count);
				tmpList.Add(srcList.ElementAt(randSongNum));
				srcList.RemoveAt(randSongNum);
			}
			return tmpList;
		}

		public static List<Song> SortByTitle(this List<Song> srcSong)
		{
			var tmpList = new List<Song>();
			var sortedNameList = new List<string>();
			int cntr = srcSong.Count;

			foreach (var song in srcSong)
			{
				sortedNameList.Add(song.Name);
			}
			sortedNameList.Sort();

			foreach (var songName in sortedNameList)
			{
				for (int i = 0; i < cntr; i++)
				{
					if (srcSong.ElementAt(i).Name == songName)
					{
						tmpList.Add(srcSong.ElementAt(i));
						srcSong.RemoveAt(i);
						break;
					}
				}
			}
			return tmpList;
		}
	}
}