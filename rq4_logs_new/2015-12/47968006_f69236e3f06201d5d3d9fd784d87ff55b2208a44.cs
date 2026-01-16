using UnityEngine;
using System.Collections;

public class ScoreDisplay : MonoBehaviour {

	public Texture[] zeroToNine = new Texture[10];
	public GameObject[] numberBlocks = new GameObject[3];
	public GameBrain GB;
	public ScoreCounterArray scoreCounterArray;



	// Use this for initialization
	void Start () {


		GB = GameObject.Find ("GameBrain").GetComponent<GameBrain> ();

		for (int i = 0; i < scoreCounterArray.scoreCounters.Length; i++) {

			int arrayFirst = ((i % 100) - (i % 10)) / 10;
			int arraySecond = i % 10;

			
			if (scoreCounterArray.scoreCounters [i] == gameObject) {

				int ones = GB.pelletscore [arrayFirst] [arraySecond] % 10; 				                // sista siffran i ett tretal 12(3)
				int tens = (GB.pelletscore [arrayFirst] [arraySecond] % 100 - ones) / 10;     	           // andra siffran 1(2)3
				int hundreds = (GB.pelletscore [arrayFirst] [arraySecond] % 1000 - tens - ones) / 100;   // tredehe siffran 12(3)



				numberBlocks [0].gameObject.GetComponent<Renderer> ().material.mainTexture = zeroToNine [hundreds];
				numberBlocks [1].gameObject.GetComponent<Renderer> ().material.mainTexture = zeroToNine [tens];
				numberBlocks [2].gameObject.GetComponent<Renderer> ().material.mainTexture = zeroToNine [ones];

				}
			}
	}
}