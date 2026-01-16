using UnityEngine;
using System.Collections;

public class Emitter : MonoBehaviour {

	public GameObject[] waves;

	private int currentWave;

	IEnumerator Start(){

		if(waves.Length == 0){
			yield break;
		}

		while(true){
			GameObject wave = (GameObject)Instantiate(waves[currentWave], transform.position, Quaternion.identity);
			wave.transform.parent = transform;

			while(wave.transform.childCount != 0){
				yield return new WaitForEndOfFrame();
			}

			Destroy(wave);

			if(waves.Length <= ++currentWave){
				currentWave = 0;
			}

		}


	}

}