using UnityEngine;
using System.Collections;

public class NPCgenerator : MonoBehaviour {
	public Sprite[] head;
	public Sprite[] arms;
	public Sprite[] trousers;
	public Sprite[] pants;
	public Sprite[] faces;
	public Sprite[] hair;
	public Sprite[] skirts;
	public Sprite[] necks;
	public Sprite[] hands;
	public Sprite[] boots;
	public Sprite[] whair;
	public GameObject[] placeHolders;

	public enum Gender{Man,Woman};
	public Gender currentGender;

	void GenerateNPC()
	{
		if (currentGender == Gender.Man) {
			placeHolders [13].GetComponent<SpriteRenderer> ().sprite = hair [Random.Range (0, 4)];
			placeHolders [14].SetActive (false);
		} else {
			placeHolders [14].GetComponent<SpriteRenderer> ().sprite = whair [Random.Range (0, 4)];
			placeHolders [13].SetActive (false);
		}

		int skinColor = Random.Range (0, 4);
		int pantsColor = Random.Range (0, 4);
		placeHolders [0].GetComponent<SpriteRenderer>().sprite = placeHolders [1].GetComponent<SpriteRenderer>().sprite = trousers [pantsColor];
		placeHolders [2].GetComponent<SpriteRenderer> ().sprite = pants [pantsColor];
		placeHolders [3].GetComponent<SpriteRenderer>().sprite = placeHolders [4].GetComponent<SpriteRenderer>().sprite = boots [Random.Range (0, 4)];
		placeHolders [5].GetComponent<SpriteRenderer> ().sprite = skirts [Random.Range (0, 4)];
		placeHolders [6].GetComponent<SpriteRenderer>().sprite = placeHolders [7].GetComponent<SpriteRenderer>().sprite = hands [Random.Range (0, 4)];
		placeHolders [8].GetComponent<SpriteRenderer> ().sprite = necks [skinColor];
		placeHolders [9].GetComponent<SpriteRenderer> ().sprite = head [skinColor];
		placeHolders [10].GetComponent<SpriteRenderer> ().sprite = placeHolders [11].GetComponent<SpriteRenderer> ().sprite = arms [skinColor];
		placeHolders [12].GetComponent<SpriteRenderer> ().sprite = faces [Random.Range (0, 4)];
	}

	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown (KeyCode.M)) {
			GenerateNPC ();
		}
	}
}