using UnityEngine;
using System.Collections;

public class graveControl : MonoBehaviour {

    public bool empty = true;
    bool dug = false;
    public float dirt = 100;
   
	void Update () {
	    
        if(dug == false && dirt <= 0)
        {
            dug = true;
            if (empty)
            {
                Destroy(this.gameObject, 1); 
                // ToDo - create used grave
            }
            else
            {
                // ToDo - get artifact 
            }
        }

	}
}