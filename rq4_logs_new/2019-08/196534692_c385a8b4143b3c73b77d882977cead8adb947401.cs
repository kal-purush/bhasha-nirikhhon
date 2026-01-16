using System;

namespace Exceptions
{
    internal struct Worker
    {
        public readonly string surname;
        public readonly string initials;
        public readonly string position;
        public readonly int year;

        public Worker(string surname, string initials, string position, int year)
        {
            this.surname = surname;
            this.initials = initials;
            this.position = position;

            if (year < 1992 || year > 2019)
            {
                throw new Exception("The year should be between 1992 and 2019");
            }
            else
            {
                this.year = year;
            }
        }

        public void Show()
        {
            Console.WriteLine("Surname  - {0}", surname);
            Console.WriteLine("Initials - {0}", initials);
            Console.WriteLine("Position - {0}", position);
            Console.WriteLine("Year     - {0}", year);
        }
    }
}