using UnityEngine;
using System.Collections;

public class Audio : MonoBehaviour
{
	public void PlaySoundOnce(AudioClip audioClip)
	{
		StartCoroutine(PlaySoundCoroutine(audioClip));
	}

	IEnumerator PlaySoundCoroutine(AudioClip audioClip)
	{
		GetComponent<AudioSource>().PlayOneShot(audioClip);
		yield return new WaitForSeconds(audioClip.length);
		Destroy(gameObject);
	}

	public void PlaySoundLoop(AudioClip audioClip)
	{
		GetComponent<AudioSource>().clip = audioClip;
		GetComponent<AudioSource>().loop = true;
		GetComponent<AudioSource>().Play();
	}

	public void StopSound()
	{
		GetComponent<AudioSource>().Stop();
		Destroy(gameObject);
	}
}