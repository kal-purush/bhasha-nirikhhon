using System;
using UnityEngine;

namespace ProceduralSkyMod
{
    class DateToSkyMapper
	{
		public static Quaternion SkyboxNightRotation { get; private set; }
		public static Quaternion SunPivotRotation { get; private set; }
		public static Vector3 SunOffset { get; private set; }
		public static Quaternion MoonRotation { get; private set; }
		public static float DayProgress { get; private set; }
		public static float YearProgress { get; private set; }

		public static void ApplyDate(DateTime clockTime)
		{
			DateTime dayStart = new DateTime(clockTime.Year, clockTime.Month, clockTime.Day);
			DateTime yearEnd = new DateTime(clockTime.Year, 12, 31);
			int daysInYear = yearEnd.DayOfYear;

			DateTime utcTime = clockTime - TimeZoneInfo.Local.GetUtcOffset(clockTime);
			if (TimeZoneInfo.Local.IsDaylightSavingTime(clockTime)) { utcTime.AddHours(-1); }
			DateTime solarTime = utcTime.AddHours(Main.settings.longitude / 15);
			TimeSpan timeSinceMidnight = solarTime.Subtract(dayStart);
			DayProgress = (float)timeSinceMidnight.TotalHours / 24;
			YearProgress = (clockTime.DayOfYear + DayProgress) / daysInYear;

			// rotating the skybox 1 extra rotation per year causes the night sky to differ between summer and winter
			float yearlyAngle = 360 * YearProgress;
			float dailyAngle = 360 * DayProgress + 180; // +180 swaps midnight & noon
			SkyboxNightRotation = Quaternion.Euler(-Main.settings.latitude, 0, (dailyAngle + yearlyAngle) % 360);
			SunPivotRotation = Quaternion.Euler(-Main.settings.latitude, 0, dailyAngle % 360);
			SunOffset = new Vector3(0, 0, -10 * Mathf.Tan(23.4f * Mathf.PI / 180 * Mathf.Cos(2 * Mathf.PI * YearProgress)));
			// moon is new when rotation around self.forward is 0
			float phaseAngle = ComputeMoonPhase(solarTime);
			MoonRotation = Quaternion.Euler(-Main.settings.latitude + 23.4f + 5.14f, 0, (dailyAngle - phaseAngle) % 360);
		}

		// phase range is [0-360)
		// taken from https://www.subsystems.us/uploads/9/8/9/4/98948044/moonphase.pdf
		private static float ComputeMoonPhase(DateTime now)
		{
			var jDays = ToJulianDays(now);
			var jDaysSinceKnownNewMoon = jDays - 2451549.5; // known new moon on 2000 January 6
			var newMoonsSinceKnownNewMoon = jDaysSinceKnownNewMoon / 29.53;
			var fractionOfCycleSinceLastNewMoon = newMoonsSinceKnownNewMoon % 1;
			return (float)(360 * fractionOfCycleSinceLastNewMoon);
		}

		private static double ToJulianDays(DateTime now)
		{
			var fractionOfDaySinceMidnight = now.Subtract(new DateTime(now.Year, now.Month, now.Day)).TotalHours / 24;
			int Y = now.Year, M = now.Month, D = now.Day;
			int A = Y / 100;
			int B = A / 4;
			int C = 2 - A + B;
			int E = (int)(365.25 * (Y + 4716));
			int F = (int)(30.6001 * (M + 1));
			return C + D + E + F - 1524.5 + fractionOfDaySinceMidnight;
		}
	}
}