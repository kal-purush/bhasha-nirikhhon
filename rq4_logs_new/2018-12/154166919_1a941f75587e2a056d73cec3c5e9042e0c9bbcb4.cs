using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class hint : MonoBehaviour {
    public GameObject roadblock_hint, tele_on, tele_off;
    bool hint_enabled = false;
    float timer;

	// Use this for initialization
	void Start () {
        timer = Time.time;
        roadblock_hint.SetActive(false);
    }
	
	// Update is called once per frame
	void Update () {
        float current_time = Time.time;
        if (current_time - timer > 30f)
        {
            tele_off.SetActive(false);
            tele_on.SetActive(true);
            roadblock_hint.SetActive(true);
        }
	}
}