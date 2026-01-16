using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Inverter : Przeszkoda_A {


	public override IEnumerator Effect()
	{
		GameManager.Instance.playerStatus[0] = true; // zapisz w tablicy, ze sterowanie jest odwrocone
		GameManager.Instance.ConfusedText.text = "You're Confused!";
		GameManager.Instance.ConfusedText.gameObject.SetActive (true);
		yield return new WaitForSeconds (GameManager.Instance.InversionDuration);
		GameManager.Instance.playerStatus[0] = false;
		GameManager.Instance.ConfusedText.gameObject.SetActive (false);
	}
}