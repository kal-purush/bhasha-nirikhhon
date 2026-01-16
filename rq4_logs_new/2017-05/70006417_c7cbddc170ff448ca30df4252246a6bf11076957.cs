using UnityEngine;
using System.Collections;

public class DirectionChecker : MonoBehaviour {

    Animator anim;
    private int direction; //left = -1, right = +1
	
	void Start () {
        anim = GetComponent<Animator>();
        direction = 1;
	}
	
    public int getDirection()
    {
        //print(direction);
       if (!anim.GetBool("roll"))
        {
            if (Input.GetAxis("Horizontal") < 0)
            {
                direction = -1;
            }
            else if (Input.GetAxis("Horizontal") > 0)
            {
                direction = 1;
            }
        }
        return direction;
    }

}