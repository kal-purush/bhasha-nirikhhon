using UnityEngine;
using System.Collections;

public class screen : MonoBehaviour {

    private Vector3 scale;
	
    void Start()
    {
        SpriteRenderer sprite;
        sprite = GetComponent<SpriteRenderer>();
     

        sprite.transform.localScale = new Vector3(1, 1, 1);

        var width = sprite.sprite.bounds.size.x;
        var height = sprite.sprite.bounds.size.y;

        var worldScreenHeight = Camera.main.orthographicSize * 2.0;
        var worldScreenWidth = worldScreenHeight / Screen.height * Screen.width;

        sprite.transform.localScale = new Vector3((float) worldScreenWidth / width, (float) worldScreenHeight / height, 1);
    }

  

     
	// Update is called once per frame
	void Update () {
	
	}
}