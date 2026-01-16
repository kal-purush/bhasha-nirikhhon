using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ObjectThrow : MonoBehaviour {

    public ThrowingArcRender tar;

    public bool start = false;

    int nextPosIndex = 0;

    Rigidbody2D rb = new Rigidbody2D();

	// Use this for initialization
	void Start () {
        rb = GetComponent<Rigidbody2D>();
    }
	
	// Update is called once per frame
	void FixedUpdate () {
        //tar.arcArray

        if (Input.GetKeyDown(KeyCode.Space) && !!!start)
        {
            transform.position = tar.arcArray[0];
            nextPosIndex = 1;
            
            start = true;
        }
        if (start)
        {
            if (nextPosIndex == tar.arcArray.Length - 1)
            {
                start = false;
            }
            try
            {
                rb.MovePosition(/*new Vector2(transform.position.x + .001f, */tar.arcArray[nextPosIndex]);
                if (transform.position.y > tar.arcArray[nextPosIndex].y - .5f)
                {
                    nextPosIndex += 6 - (int)Mathf.Ceil(5 * transform.position.y / (tar.apexY - tar.pexY));

                }
                else
                {
                    Debug.Log("still going");
                }
            }
            catch (System.IndexOutOfRangeException e)
            {
                Debug.Log("error");
                rb.MovePosition(tar.arcArray[tar.arcArray.Length - 1]);
                start = false;
            }
            
        }
	}
}