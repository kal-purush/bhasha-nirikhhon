using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MenuIconBehavior : MonoBehaviour {

    //Used to declare the first object and update mouse position for everyone
    private static GameObject leader;
    private bool isLeader;
    public static Vector2 mousePosition = new Vector2();

    public Sprite unlockedSprite;

	// Use this for initialization
	void Start () {
        this.GetComponent<SpriteRenderer>().sprite = unlockedSprite;

        if(leader == null)
        {
            leader = this.gameObject;
            isLeader = true;
        }
        else
        {
            isLeader = false;
        }
	}
	
	// Update is called once per frame
	void Update () {
		if(isLeader)
        {
            mousePosition = GlobalMethods.MousePoint.ToVector2();
        }
	}
}