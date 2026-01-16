using System.Collections;
using UnityEngine;

namespace ProceduralSkyMod
{
	public class WeatherSource
	{
		private static float cloudTarget = 2, cloudCurrent = 1;

		public static float SkyClarity { get { cloudCurrent = Mathf.Lerp(cloudCurrent, cloudTarget, Time.deltaTime * 0.1f); return cloudCurrent; } }

		public static IEnumerator CloudChanger ()
		{
			while (true)
			{
				yield return new WaitForSeconds(60);
				// .5 to 5 to test it
				cloudTarget = Mathf.Clamp(Random.value * 5, .5f, 5f);
#if DEBUG
				Debug.Log(string.Format("New Cloud Target of {0}, current {1}", cloudTarget, cloudCurrent));
#endif
			}
		}
	}
}