using System;

namespace DateTest
{
    partial class Program
    {
        public class Date
        {
            private int _day;
            private int _month;
            private int _year;

            public Date(int day, int month, int year)
            {
                this._day = day;
                this._month = month;
                this._year = year;
            }

            public void SetDay(int day)
            {
                this._day = day;
            }

            public void SetMonth(int month)
            {
                this._month = month;             
            }

            public void SetYear(int year)
            {
                this._year = year;
            }

            public int GetYear()
            {
                return this._year;
            }

            public int GetMonth()
            {
                return this._month;
            }
            public int GetDay()
            {
                return this._day;
            }

            public void DisplayDate()
            {
                Console.WriteLine($"{this._month} / {this._day} / {this._year}");
            }
        }
    }
}