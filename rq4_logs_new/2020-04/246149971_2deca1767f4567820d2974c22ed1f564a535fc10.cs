using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Boss : MonoBehaviour
{
	public Slider bossHealthBar;
	
	public int vidaBoss;
	public int danoLateral;
	
	// public Animator ataqueBoss;
	// public Animator camAnim;
	// public Animator danoBoss;

	public ParticleSystem particulaAtaque;

	public List<GameObject> backTiles = new List<GameObject>();
	
	// Dictionary<string, GameObject> myDictionaryObjects = new Dictionary<string, GameObject>();

	private void Start()
	{
		BehindAttack();
	}

	private void Update()
	{
		// bossHealthBar.value = vidaBoss;
	}

 	public void BehindAttack()
 	{
 		// Animate Dragon's attack

 		
 		foreach(GameObject tile in backTiles)
 		{
 			// Quaternion rotationParticula = new Quaternion(tile.transform.rotation.x, tile.transform.rotation.y, tile.transform.rotation.z, 0f );
 			Quaternion rotationParticula = new Quaternion( -90f, tile.transform.rotation.y, tile.transform.rotation.z, 0f );
            // Instantiate(particulaFogo, tile.transform.position, tile.transform.rotation);
            Instantiate(particulaAtaque, tile.transform.position, rotationParticula);
 		}
 	}

 	public void SideAttack()
 	{

 	}
}