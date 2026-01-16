using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CharShopButtons : MonoBehaviour {

    public GameObject _page;

    private Vector2 _point = new Vector2(1080,0);

	// Use this for initialization
	void Start () {

	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public void RightButton()
    {
        _page.transform.localPosition -= (Vector3)_point;
    }

    public void LeftButton()
    {
        _page.transform.localPosition += (Vector3)_point;
    }
}