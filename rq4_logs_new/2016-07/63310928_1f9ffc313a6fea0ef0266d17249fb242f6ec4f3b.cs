using UnityEngine;
using System.Collections;

public class NGUU : MonoBehaviour {
   
    public int a;
    int b=0;
	// Use this for initialization
	void Start () {
        for (b = 0; b <= a; b++) {
            if (b % 2 == 0)
            {
                Debug.Log(b);
            }
       }
    }
}