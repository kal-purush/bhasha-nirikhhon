using UnityEngine;
using UnityEngine.Networking;

public class PlayerSetup : NetworkBehaviour
{
	[SerializeField] private Behaviour[] componentsToDisable;

	UnityEngine.Camera sceneCamera;

	// Use this for initialization
	 void Start () 
	{
		if (!isLocalPlayer)
		{
			for (var i = 0; i < componentsToDisable.Length; i++)
			{
				componentsToDisable[i].enabled = false;
			}
		}
		else
		{
			sceneCamera = UnityEngine.Camera.main;
			if (sceneCamera != null)
			{
				sceneCamera.gameObject.SetActive(false);
			}
		}
	}

	private void OnDisable()
	{
		if (sceneCamera != null)
		{
			sceneCamera.gameObject.SetActive(true);
		}
	}
}