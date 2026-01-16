using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAttack : IPlayerAction
{

	public void Execute (GameObject go, int attackValue)
	{
		if (GameManager.Instance.attackerName.Equals (GameManager.Instance.userName)) {
			if (GameManager.Instance.isPlayerVisitor) {
				go.GetComponent<BattleManager> ().homeLife -= attackValue;
			} else {
				go.GetComponent<BattleManager> ().visitorLife -= attackValue;
			}
		} else {
			if (GameManager.Instance.isPlayerVisitor) {
				go.GetComponent<BattleManager> ().visitorLife -= attackValue;
			} else {
				go.GetComponent<BattleManager> ().homeLife -= attackValue;
			}
		}
		
	}
}