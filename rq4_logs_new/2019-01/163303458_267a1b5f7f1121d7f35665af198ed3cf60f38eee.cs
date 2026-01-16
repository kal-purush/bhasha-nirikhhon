using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace MusicPlayer.Extantions
{
    public static class PlayerHelper
    {
        public static List<Song> SortSongs(this List<Song> songs)           //L9 -HW -Player -1/3
        {
            List<string> localList = new List<string>(songs.Count);
            int index = 0;
            foreach (var song in songs)
            {
                localList.Insert(index, song.Name);
                index++;
            }
            localList.Sort();

            List<Song> songLocal = new List<Song>(songs.Count);
            index = 0;
            foreach (var item in localList)
            {
                for (int i = 0; i < songs.Count; i++)
                {
                    if (item == songs[i].Name)
                    {
                        songLocal.Insert(index, songs[i]);
                        index++;
                    }
                }
            }
            return songLocal;
        }

        public static List<T> ShuffleItem<T> (this List<T> item)
        {
            List<T> itemShuffle = new List<T>(item.Count);
            int indexList = 0;
            bool flag = true;
            for (int i = 0, j = (item.Count - 1); i <= j;)
            {
                if (flag)
                {
                    itemShuffle.Insert(indexList, item[j]);
                    j--;
                    indexList++;
                    flag = false;
                }
                else
                {
                    itemShuffle.Insert(indexList, item[i]);
                    i++;
                    indexList++;
                    flag = true;
                }
            }
            return itemShuffle;
        }
        public static string PlayerSubstring (this string @string)              //L9 -HW -Player -2/3 .
        {
            return (@string.Substring(0, 3) + "...");
        }
    }
}