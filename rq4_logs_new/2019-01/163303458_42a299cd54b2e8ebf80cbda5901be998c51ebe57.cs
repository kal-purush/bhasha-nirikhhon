using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Base.Lesson_5
{
    class Program  
    {
        static void Main(string[] args)
        {
            //ArrayListExample();               //B7-P1/5. ArrayListPoemSort
            //ArrayListOfSongsSort();           //B7-P2/5. ArrayListOfSongsSort
            //GenericListOfSongsSort();         //B7-P3/5. GenericListOfSongsSort
            //GenericListOfNeighborSearch();    //B7-P4/5. GenericListOfNeighborSearch
            DictionaryOfNeighborSearch();       //B7-P5/5. DictionaryOfNeighborSearch

            Console.ReadLine();
        }

        public static void ArrayListExample()
        {
            var poem = new ArrayList();

            for (int i = 0; i < 3; i++)
            {
                Console.WriteLine("Next row please");
                poem.Add(Console.ReadLine());
            }
            poem.Sort();

            foreach (var row in poem)
            {
                Console.WriteLine(row);
            }
            poem.RemoveAt(poem.Count - 1);

            object[] myArray = poem.ToArray();

            foreach (var item in myArray)
            {
                Console.WriteLine(item);
            }

        }

        public static void ArrayListOfSongsSort()
        {
            var poem = new ArrayList();
            for (int i = 0; i < 5; i++)
            {
                var song = new Song();
                song.Lyrics = Console.ReadLine();
                poem.Add(song);
            }

            //poem.Sort();

            foreach (var row in poem)
            {
                Console.WriteLine(row);
            }
            poem.RemoveAt(poem.Count - 1);

            //object[] myArray = poem.ToArray();

            //foreach (var item in myArray)
            //{
            //    Console.WriteLine(item);
            //}
        }

        public static void GenericListOfSongsSort()
        {
            List<Song> songs = new List<Song>();
            for (int i = 0; i < 5; i++)
            {
                var song = new Song();
                song.Lyrics = Console.ReadLine();
                songs.Add(song);
            }

            songs.Sort();

            foreach (var row in songs)
            {
                Console.WriteLine(row.Lyrics);
            }
            songs.RemoveAt(songs.Count - 1);
        }

        public static void GenericListOfNeighborSearch()
        {
            List<Neighbor> floorNeighbors = new List<Neighbor>();
            for (int i = 0; i < 5; i++)
            {
                var neighbor = new Neighbor();
                Console.WriteLine("Full Name");
                neighbor.FullName = Console.ReadLine();
                Console.WriteLine("Flat Number");
                neighbor.FlatNumber = Convert.ToInt32(Console.ReadLine());
                Console.WriteLine("Phone Number");
                neighbor.PhoneNumber = Convert.ToInt32(Console.ReadLine());
                floorNeighbors.Add(neighbor);
            }
            Console.WriteLine("Enter number of flat");
            int tempFlat = Convert.ToInt32(Console.ReadLine());
            foreach (var neighbor in floorNeighbors)
            {
                if (neighbor.FlatNumber == tempFlat)
                {
                    Console.WriteLine($"{neighbor.PhoneNumber}, {neighbor.FullName}");
                }
            }
        }

        public static void DictionaryOfNeighborSearch()
        {
            //List<Neighbor> floorNeighbors = new List<Neighbor>();
            Dictionary<int, Neighbor> floorNeighbors = new Dictionary<int, Neighbor>();
            for (int i = 0; i < 5; i++)
            {
                var neighbor = new Neighbor();
                Console.WriteLine("Full Name");
                neighbor.FullName = Console.ReadLine();
                Console.WriteLine("Flat Number");
                neighbor.FlatNumber = Convert.ToInt32(Console.ReadLine());
                Console.WriteLine("Phone Number");
                neighbor.PhoneNumber = Convert.ToInt32(Console.ReadLine());
                floorNeighbors.Add(neighbor.FlatNumber, neighbor);
            }
            Console.WriteLine("Enter number of flat");
            int tempFlat = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine($"For key {tempFlat}, value {floorNeighbors[tempFlat].PhoneNumber}, {floorNeighbors[tempFlat].FullName}");
            //foreach (var neighbor in floorNeighbors)
            //{
            //    if (neighbor.FlatNumber == tempFlat)
            //    {
            //        Console.WriteLine($"{neighbor.PhoneNumber}, {neighbor.FullName}");
            //    }
            //}
        }

        #region Song : IComparable
        public class Song : IComparable
        {
            public string Lyrics;

            //public int CompareTo(object obj)
            //{
            //    if (Lyrics == null)
            //    {
            //        throw new ArgumentNullException(nameof(obj));
            //    }
            //    Song otherSong = obj as Song;
            //    if (otherSong != null) return this.Lyrics.CompareTo(otherSong.Lyrics);
            //}

            public int CompareTo(object obj)
            {
                Song otherSong = obj as Song;
                return this.Lyrics.CompareTo(otherSong.Lyrics);
            }

            public override string ToString()
            {
                return this.Lyrics;
            }
        }
        #endregion

        #region Neighbor
        public class Neighbor //: IComparable
        {
            public string FullName;
            public int FlatNumber;
            public int PhoneNumber;

            //public int CompareTo(object obj)
            //{
            //    Song otherSong = obj as Song;
            //    return this.Lyrics.CompareTo(otherSong.Lyrics);
            //}

            //public override string ToString()
            //{
            //    return this.Lyrics;
            //}
        }
        #endregion
    }
}