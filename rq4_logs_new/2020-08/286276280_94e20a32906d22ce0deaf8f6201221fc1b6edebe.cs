using System;
using System.IO;
using UnityEngine;

namespace ProceduralSkyMod
{
	[Serializable]
	public class SkySaveData
	{
		public float dayProgress = 0;
		public float yearProgress = 0;
		public Vector3 skyRotation = Vector3.zero;
		public Vector3 sunRotation = Vector3.zero;
		public Vector3 moonRotation = Vector3.zero;
	}

	public static class SkySaveLoad
	{
		public static void Save ()
		{
			try
			{
				string path = Path.Combine(Main.Path, "SkySave.json");
				if (!File.Exists(path)) File.Create(path).Close();

				SkyManager instance = GameObject.Find("ProceduralSkyMod").GetComponent<SkyManager>();

				SkySaveData state = new SkySaveData();
				state.dayProgress = TimeSource.DayProgress;
				state.yearProgress = TimeSource.YearProgress;
				state.skyRotation = instance.SkyboxNight.eulerAngles;
				state.sunRotation = instance.SunPivot.eulerAngles;
				state.moonRotation = instance.MoonBillboard.eulerAngles;

				File.WriteAllText(path, JsonUtility.ToJson(state));

				Debug.Log(string.Format("Saved Procedural Sky State: {0}", path));
			}
			catch (Exception ex)
			{
				Debug.Log(ex);
				throw;
			}
		}

		public static SkySaveData Load ()
		{
			try
			{
				string path = Path.Combine(Main.Path, "SkySave.json");
				if (!File.Exists(path))
				{
					File.Create(path).Close();

					SkyManager instance = GameObject.Find("ProceduralSkyMod").GetComponent<SkyManager>();

					SkySaveData state = new SkySaveData();
					state.dayProgress = 0.5f;
					state.yearProgress = 0.5f;
					state.skyRotation = new Vector3(-instance.latitude, 0, 0);
					state.sunRotation = Vector3.zero;
					state.moonRotation = new Vector3(0, 0, 180f);

					File.WriteAllText(path, JsonUtility.ToJson(state));
					return state;
				}
				else
				{
					return JsonUtility.FromJson<SkySaveData>(File.ReadAllText(path));
				}
			}
			catch (Exception ex)
			{
				Debug.Log(ex);
				throw;
			}
		}
	}
}