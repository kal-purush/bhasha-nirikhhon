using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static hm_09.Song;

namespace hm_09
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("Введите название песни:");
            string str1 = Console.ReadLine();

            Console.Write("Введите продолжительность песни (в целых минутах):");
            int timeSong = Convert.ToInt32(Console.ReadLine());

            Console.Write("Укажите жанр песни: нет типа(notType-0), классичекая (classical-1), тяжелая (heavy-2), рэп(rap-3), рок(rock-4).");
            int genreSong = Convert.ToInt32(Console.ReadLine());

            Console.Write("Введите год написание песни песни:");
            int yearSong = Convert.ToInt32(Console.ReadLine());

            GenreSong SongGener = (GenreSong)genreSong;

            Song song = new Song(str1, timeSong, yearSong, SongGener);

            string json = JsonConvert.SerializeObject(song.GetSongData(song));

            Console.WriteLine(song.GetSongData(song));

            Console.WriteLine(json);

            Console.ReadLine();
        }
    }
}