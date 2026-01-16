using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ChangeSprite : MonoBehaviour {
    public static int Num;
    public GameObject[] obj = new GameObject[Num];
    public Sprite[] sprite = new Sprite[Num];
	// Use this for initialization
	void Start () {
        Num = obj.Length;
	}
	public void SpritesChange()
    {
        int i;
        for(i=0; i< Num; i++)
        {
            if (obj[i] && sprite[i])
            {
                obj[i].GetComponent<SpriteRenderer>().sprite = sprite[i];
                Debug.Log("ChangeSprite :" + obj[i].name + " & " + sprite[i].name);
            }
        }
    }
	// Update is called once per frame
	void Update () {
      
	}

   
}