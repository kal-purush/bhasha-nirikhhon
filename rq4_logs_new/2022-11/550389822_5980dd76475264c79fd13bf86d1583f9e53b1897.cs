using System;
using System.Globalization;

namespace Time
{
  public enum Location { NewYork, London, Paris }
  public enum AlertLevel { Early, Standard, Late }

  public static class Appointment
  {
    public static DateTime ShowLocalTime(DateTime dtUtc) => dtUtc.ToLocalTime();
    public static DateTime Schedule(string appointmentDateDescription, Location location)
    {
      DateTime dtUtc;
      try
      {
        dtUtc = DateTime.Parse(appointmentDateDescription);
        return TimeZoneInfo.ConvertTimeToUtc(dtUtc, TZInfo(location));
      }
      catch
      {
        throw;  // 再スロー
      }
    }

    public static DateTime GetAlertTime(DateTime appointment, AlertLevel alertLevel)
    {
      var beforeTime = alertLevel switch
      {
        AlertLevel.Early => new TimeSpan(24, 0, 0),
        AlertLevel.Standard => new TimeSpan(1, 45, 0),
        AlertLevel.Late => new TimeSpan(0, 30, 0),
        _ => throw new ArgumentOutOfRangeException(),
      };
      return appointment - beforeTime;
    }

    public static bool HasDaylightSavingChanged(DateTime dt, Location location)
    {
      TimeZoneInfo tst = TZInfo(location);
      bool isRecentChanged =
        tst.IsDaylightSavingTime(dt) != tst.IsDaylightSavingTime(dt.AddDays(-7));
      return isRecentChanged;
    }

    public static DateTime NormalizeDateTime(string dtStr, Location location)
    {
      CultureInfo ci = location switch
      {
        // CultureInfo には Invariant, Neutral, Specific の3タイプがある
        // format には Specific が求められる
        // CultureInfo.GetCultureInfo は Neutral もあり得る
        Location.NewYork => CultureInfo.CreateSpecificCulture("en-US"),
        Location.London => CultureInfo.CreateSpecificCulture("en-GB"),
        Location.Paris => CultureInfo.CreateSpecificCulture("fr-FR"),
        _ => throw new ArgumentOutOfRangeException(),
      };
      DateTime dt;
      try
      {
        dt = DateTime.Parse(dtStr, ci);
      }
      catch
      {
        dt = new DateTime(1, 1, 1);
      }
      return dt;
    }
    private static TimeZoneInfo TZInfo(Location location)
    {
      string zoneid;
      // using System.Runtime.InteropServices が必要になる
      // RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
      if (OperatingSystem.IsWindows())  // コチラなら不要
      {
        zoneid = location switch
        {
          Location.NewYork => "Eastern Standard Time",
          Location.London => "GMT Standard Time",
          Location.Paris => "W. Europe Standard Time",
          _ => throw new ArgumentOutOfRangeException(),
        };
      }
      else
      {
        zoneid = location switch
        {
          Location.NewYork => "America/New_York",
          Location.London => "Europe/London",
          Location.Paris => "Europe/Paris",
          _ => throw new ArgumentOutOfRangeException(),
        };
      }
      return TimeZoneInfo.FindSystemTimeZoneById(zoneid);
    }
  }
}
