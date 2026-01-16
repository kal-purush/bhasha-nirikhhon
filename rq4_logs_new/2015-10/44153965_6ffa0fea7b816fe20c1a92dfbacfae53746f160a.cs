using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class DuxBalance : MonoBehaviour {

    public Text accountBalance;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        accountBalance.text = CurrencySystem.Get().ToString();
	}
}