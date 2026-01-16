using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;
using UnityEngine;

public class ManaManager : MonoBehaviour {

    public Image manaBar;
    public float value;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        CheckBar();
	}

    public void CheckBar() {
        manaBar.rectTransform.localScale = new Vector3(value / 100, manaBar.rectTransform.localScale.y, manaBar.rectTransform.localScale.z);
    }

    public void AddMana (float amount) {
        if (value + amount > 100.0f) {
            value = 100.0f;
        } else {
            value += amount;
        }
    }

    public void SubMana (float amount) {
        if (value - amount < 0.0f) {
            value = 0.0f;
        } else {
            value -= amount;
        }
    }
}