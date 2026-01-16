using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace ProceduralSkyMod
{
	public class RainController
	{
		public static ParticleSystem[] RainParticleSystems { get; set; }

		private static int[] maxParticleEmission;
		private static ParticleSystem.ShapeModule shapeModule;

		public static void SetRainParticleSystemArray (ParticleSystem[] systems)
		{
			RainParticleSystems = new ParticleSystem[3];
			maxParticleEmission = new int[3];

			for (int i = 0; i < systems.Length; i++)
			{
				if (systems[i].gameObject.name.Contains("RainDrop"))
				{
					RainParticleSystems[0] = systems[i];
					maxParticleEmission[0] = (int)systems[i].emission.rateOverTime.constant;
					RainParticleSystems[0].Play();
				}
				else if (systems[i].gameObject.name.Contains("RainCluster"))
				{
					RainParticleSystems[1] = systems[i];
					maxParticleEmission[1] = (int)systems[i].emission.rateOverTime.constant;
					RainParticleSystems[1].Play();
				}
				else if (systems[i].gameObject.name.Contains("RainHaze"))
				{
					RainParticleSystems[2] = systems[i];
					maxParticleEmission[2] = (int)systems[i].emission.rateOverTime.constant;
					RainParticleSystems[2].Play();
				}
				else Debug.LogWarning(string.Format("ProSkyMod RainParticleController ERR-00: No name match for {0}", RainParticleSystems[i].gameObject.name));
			}
		}

		public static void SetShapeTextures ()
		{
			shapeModule = RainParticleSystems[0].shape;
			shapeModule.texture = WeatherSource.CloudRenderImage0;
			shapeModule = RainParticleSystems[1].shape;
			shapeModule.texture = WeatherSource.CloudRenderImage1;
			shapeModule = RainParticleSystems[2].shape;
			shapeModule.texture = WeatherSource.CloudRenderImage2;
		}

		public static void SetRainStrength (float strength)
		{
			Debug.Log("FOO");
			ParticleSystem.EmissionModule module = RainParticleSystems[0].emission;
			module.rateOverTime = maxParticleEmission[0] * strength;
			module = RainParticleSystems[1].emission;
			module.rateOverTime = maxParticleEmission[1] * strength;
			module = RainParticleSystems[2].emission;
			module.rateOverTime = maxParticleEmission[2] * strength;
		}
	}
}