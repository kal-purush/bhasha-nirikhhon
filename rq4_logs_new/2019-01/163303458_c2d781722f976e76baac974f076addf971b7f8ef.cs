using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Advanced_Lesson_3_Static_Interface
{
    public partial class Lesson
    {
        public static void ShuffleExample()
        {
            var songs = new List<Song>();
            var cars = new List<Car>();
            var cards = new List<string>();

            songs = Shuffle(songs);
            cars = Shuffle(cars);
            cards = Shuffle(cards);
            
            var items = new List<object>();
            foreach (var car in cars)
            {
                items.Add((object)car);
            }
            items = Shuffle(items);
        }

        public static void GenericShuffleExample()
        {
            var songs = new List<Song>();
            var cars = new List<Car>();
            var cards = new List<string>();

            songs = Shuffle<Song>(songs);
            cars = Shuffle<Car>(cars);
            cards = Shuffle<string>(cards);
        }

        public static void GenericWithConstrainExample()
        {           
            var cars = new List<Car>();
            var marines = new List<Marine>();

            cars = Sort<Car>(cars);
            marines = Sort<Marine>(marines);
        }

        public static List<T> Shuffle<T>(List<T> items)
        {
            List<T> suffledItems = new List<T>();
            int step = 3;
            for (int i = 0; i < step; i++)
            {
                int index = i;

                while (index < items.Count)
                {
                    suffledItems.Add(items[index]);
                    index += step;
                }
            }

            return suffledItems;
        }

        public static List<T> Sort<T>(List<T> items) where T: ITransport
        {
            items.Sort((t1, t2) => t1.MileAge.CompareTo(t2.MileAge));
            return items;
        }

        public static List<Song> Shuffle(List<Song> songs)
        {
            List<Song> suffledItems = new List<Song>();
            int step = 3;
            for (int i = 0; i < step; i++)
            {
                int index = i;

                while (index < songs.Count)
                {
                    suffledItems.Add(songs[index]);
                    index += step;
                }
            }

            return suffledItems;
        }

        public static List<string> Shuffle(List<string> cards)
        {
            List<string> suffledItems = new List<string>();
            int step = 3;
            for (int i = 0; i < step; i++)
            {
                int index = i;

                while (index < cards.Count)
                {
                    suffledItems.Add(cards[index]);
                    index += step;
                }
            }

            return suffledItems;
        }

        public static List<Car> Shuffle(List<Car> cars)
        {
            List<Car> suffledItems = new List<Car>();
            int step = 3;
            for (int i = 0; i < step; i++)
            {
                int index = i;

                while (index < cars.Count)
                {
                    suffledItems.Add(cars[index]);
                    index += step;
                }
            }

            return suffledItems;
        }

        public static List<object> Shuffle(List<object> items)
        {
            List<object> suffledItems = new List<object>();
            int step = 3;
            for (int i = 0; i < step; i++)
            {
                int index = i;

                while (index < items.Count)
                {
                    suffledItems.Add(items[index]);
                    index += step;
                }
            }

            return suffledItems;
        }

        public class Song
        {
            public string Title { get; set; }
        }
    }    
}