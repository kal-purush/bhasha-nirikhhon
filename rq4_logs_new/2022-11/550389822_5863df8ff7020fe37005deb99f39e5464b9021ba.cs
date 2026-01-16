using System;
using System.Globalization;

namespace Exercism.StringFormatting
{
  public static class HighSchoolSweethearts
  {
    public static string DisplaySingleLine(string studentA, string studentB)
    {
      string prior = new String(' ', 29 - studentA.Length) + studentA;
      string lator = studentB + new String(' ', 29 - studentB.Length);
      return $"{prior} ♡ {lator}";
    }

    public static string DisplayBanner(string studentA, string studentB)
    {
      string heart = String.Format(
      @"
     ******       ******
   **      **   **      **
 **         ** **         **
**            *            **
**                         **
**     {0} +  {1}    **
 **                       **
   **                   **
     **               **
       **           **
         **       **
           **   **
             ***
              *
", studentA, studentB);
      return heart;
    }

    public static string DisplayGermanExchangeStudents(string studentA
        , string studentB, DateTime start, float hours)
    {
      CultureInfo ci = CultureInfo.CreateSpecificCulture("de-DE");
      string DateStr = start.ToString("d", ci);
      string HourStr = hours.ToString("N2", ci);
      return $"{studentA} and {studentB} have been dating since {DateStr} - that's {HourStr} hours";
    }
  }
}
