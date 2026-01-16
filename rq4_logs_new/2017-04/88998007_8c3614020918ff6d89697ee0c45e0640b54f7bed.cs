using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Box : MonoBehaviour {

    public bool amCool;

	void Awake () {

	}
	
	void Update () {
        if (gameObject.transform.position.y <= -10f) Destroy(gameObject);
	}

    void OnMouseDown()
    {
        if (Game.Instance.coolMode)
        {
            if (amCool)
            {
                // points
                Game.Instance.score++;
            }
            else
            {
                // lose points
                Game.Instance.score--;
            }
        }
        else
        {
            if (amCool)
            {
                // lose points
                Game.Instance.score--;
            }
            else
            {
                // points
                Game.Instance.score++;
            }
        }
        Destroy(gameObject);
    }

}