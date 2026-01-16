using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace ProceduralSkyMod
{
	public class ReflectionProbeUpdater
	{
		// TODO: find a way to record the time the probe was updated last by DV itself and wait with the update til the timedelay threshold is reached
		//       preventing that way unnecessary renders if a.e. the player is moving and the probe is updated frequently enough

		public static ReflectionProbe probe;
		public static float probeUpdateDelay = 1f;

		public static IEnumerator UpdateProbe ()
		{
			while (true)
			{
				probe.RenderProbe();
				yield return new WaitForSeconds(probeUpdateDelay);
			}
		}
	}
}