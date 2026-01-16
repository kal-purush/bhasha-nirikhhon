using System;

namespace SwitchOnEnum
{
    public enum Season
    {
        Winter,
        Spring,
        Summer,
        Autumn
    }

    static class Program
    {
        static void Main()
        {
            DateTime date = ReadDate();
            Season season = GetSeason(date);
            string song;

            switch (season)
            {
                case Season.Winter:
                    song = "Ada Milea - Bradu' a murit";
                    break;
                    //todo: how about the other seasons? fill them all in
                default:
                    throw new InvalidOperationException("after handling all seasons above, this code becomes unreachable");
            }

            Console.WriteLine("Here's a song for " + season + ": " + song);
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static DateTime ReadDate()
        {
            DateTime date;
            bool dateEntered;
            do
            {
                Console.WriteLine(
@"Give me a date, and I'll give you a song
e.g. 
31 jan 1989
2525-12-25");
                var input = Console.ReadLine();
                dateEntered = DateTime.TryParse(input, out date);
            } 
            while (!dateEntered);

            return date;
        }

        static Season GetSeason(DateTime dateTime)
        {
            switch (dateTime.Month)
            {
                case 12:
                case 1:
                case 2:
                    return Season.Winter;
                case 3:
                case 4:
                case 5:
                    return Season.Spring;
                case 6:
                case 7:
                case 8:
                    return Season.Summer;
                case 9:
                case 10:
                case 11:
                    return Season.Autumn;
                default:
                    throw new ArgumentOutOfRangeException("dateTime", "unreachable code");
            }
        }
    }
}