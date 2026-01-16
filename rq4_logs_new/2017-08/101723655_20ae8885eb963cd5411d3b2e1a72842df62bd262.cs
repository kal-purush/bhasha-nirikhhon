using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerMove : MonoBehaviour {


    private static int _movedirection;

    public GameObject stage;

    // Use this for initialization
    void Start () {
        _movedirection = 0;
	}
	
	// Update is called once per frame
	void Update ()
    {
        _movedirection = 0;
        if (stage.GetComponent<Stage>().isthrough("left") && Input.GetKeyDown(KeyCode.LeftArrow))
        {
            gameObject.transform.Translate(new Vector2(-32,0));
            _movedirection = 1;
        }
        if (stage.GetComponent<Stage>().isthrough("right") && Input.GetKeyDown(KeyCode.RightArrow))
        {
            gameObject.transform.Translate(new Vector2(32, 0));
            _movedirection = 2;
        }
        if (stage.GetComponent<Stage>().isthrough("up") && Input.GetKeyDown(KeyCode.UpArrow) )
        {
            gameObject.transform.Translate(new Vector2(0, 32));
            _movedirection = 3;
        }
        if (stage.GetComponent<Stage>().isthrough("down") && Input.GetKeyDown(KeyCode.DownArrow))
        {
            gameObject.transform.Translate(new Vector2(0, -32));
            _movedirection = 4;
        }
    }
    public int getMoveDirection()
    {
        return _movedirection;
    }
}