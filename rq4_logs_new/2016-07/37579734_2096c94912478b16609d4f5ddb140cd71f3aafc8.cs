using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sdx.Util
{
  public class DateTimeSpan
  {
    private DateTime startDate;
    private DateTime endDate;

    public DateTimeSpan(DateTime startDate, DateTime endDate)
    {
      this.startDate = startDate;
      this.endDate = endDate;
    }

    public void EachDay(int day, Action<DateTime> action)
    {
      var current = startDate;
      if (current.CompareTo(endDate) > 0)//減らしてく
      {
        while (current.CompareTo(endDate) >= 0)
        {
          action(current);
          current = current.AddDays(-1);
        }
      }
      else if (current.CompareTo(endDate) < 0)//増やしてく
      {
        while (current.CompareTo(endDate) <= 0)
        {
          action(current);
          current = current.AddDays(1);
        }
      }
      else
      {
        action(current);
      }
    }
  }
}