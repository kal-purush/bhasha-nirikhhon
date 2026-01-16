//using Microsoft.VisualBasic;
//using System.Transactions;

Console.WriteLine("Enter passenger date of birth (dd/mm/yyyy) :");
DateTime d = DateTime.Parse(Console.ReadLine());

TimeSpan lifetime = DateTime.Now - d;

// Age in hours.

double hours = lifetime.TotalHours;
Console.WriteLine("You have been alive for " + Convert.ToInt32(hours) + " hours.");

// Age in days.


double day = lifetime.Days;
Console.WriteLine("You have been alive for " + Convert.ToInt32(day) + " days.");

// Age in days.


double week = day / 7;
Console.WriteLine("You have been alive for " + Convert.ToInt32(week) + " weeks.");

// Age in months.

double month = day / 31;
Console.WriteLine("You have been alive for " + Convert.ToInt32(month) + " months.");

// Age in years.

double year = day / 365;
Console.WriteLine("You have been alive for " + Convert.ToInt32(year) + " years.");


//double leap = d.Year % 4;
if (d.Year % 4 ==0)
{
    Console.WriteLine("You were born in a leapyear.");
}
else
{
    Console.WriteLine("You were not born in a leap year.");
}







