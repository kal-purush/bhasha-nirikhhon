using UnityEngine;
using System.Collections;

public class LessGasUpgrade : MonoBehaviour {
	public UnityEngine.UI.Text lessGassUpgradeText;
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		lessGassUpgradeText.text = "Current Level: " + Upgrades.lessGasUpgrade;
	}

	public void Clicked(){
		if(Coin.coinCount>=5){
			Coin.coinCount = Coin.coinCount - 5;	
			Upgrades.lessGasUpgrade = Upgrades.lessGasUpgrade + 1;
			if (Upgrades.lessGasUpgrade > 1)
				Upgrades.lessGasUpgrade = 2;
		}
	}
}