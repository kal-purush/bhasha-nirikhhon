using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShakeChopstick : MonoBehaviour {
    public GameObject[] Tools = new GameObject[3];
    private int now = 0;
	// Use this for initialization
	void Start () {
        for(int i = 0; i< Tools.Length; i++)
        {
            GameObject t = Tools[i];
            if (t == null)
                Debug.Log(this.name + "摇晃道具未设置！");
            if (i != 0)
                t.SetActive(false);
        }
        Tools[now].SetActive(true);
	}
	
	// Update is called once per frame
	void Update () {
		
	}
    private void ShakeOneTime()
    {
        Tools[now].SetActive(false);
        now = (now + 1) % Tools.Length;
        Tools[now].SetActive(true);
    }
    private void Shake()
    {
        Invoke("ShakeOneTime", 1);
        Invoke("ShakeOneTime", 1);
        Invoke("ShakeOneTime", 1);
    }

}