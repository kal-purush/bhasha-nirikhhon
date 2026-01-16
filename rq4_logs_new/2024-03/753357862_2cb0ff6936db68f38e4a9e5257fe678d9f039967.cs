using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.VFX;

public class VFXManager : MonoBehaviour
{
	#region References
	public static VFXManager Instance;
	[SerializeField] private VisualEffect smoke1, smoke2;
	#endregion

	private void Awake()
	{
		if (Instance == null)
		{
			Instance = this;
		} else
		{
			Destroy(gameObject);
		}
	}

	public void SmokeTransition()
	{
		smoke1.Play();
		smoke2.Play();
	}
}