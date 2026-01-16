using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DropingItem : MonoBehaviour
{
	public static bool isThrowed;
    public static bool isThrowTrig;
    // Start is called before the first frame update
    void OnEnable()
    {
        isThrowed = false;
        isThrowTrig = false;
    }

    // Update is called once per frame
    void Update()
    {
        Debug.Log("isThrowed is " + isThrowed);
    }
    void Droped() {
    	isThrowed = true;
    }
    void Trigger() {
        isThrowTrig = true;
    }
}