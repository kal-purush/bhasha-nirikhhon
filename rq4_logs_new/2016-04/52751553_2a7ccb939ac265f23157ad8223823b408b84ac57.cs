using UnityEngine;
using System.Collections;

public class DarknessEffects : Effects 
{

	[SerializeField] SpriteFaderEffects bgEffect;
	[SerializeField] SpriteFaderEffects spikesEffect;

	public override void Play ()
	{
		if(IsPlaying)
			return;
		
		base.Play();

		bgEffect.Play();
		spikesEffect.Play();
	}

	public void SetDarkness(float level)
	{
		bgEffect.maxAlpha = level;
		bgEffect.minAlpha = 0f;
		bgEffect.duration = duration;

		spikesEffect.maxAlpha = level;
		spikesEffect.minAlpha = 0f;
		spikesEffect.duration = duration/2f;

		if(level > 0.5f)
		{
			bgEffect.minAlpha = bgEffect.maxAlpha/3f;
			spikesEffect.minAlpha = spikesEffect.maxAlpha/3f;
		}

		this.Play();

	}

}