using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Destroyer : MonoBehaviour
{
    public float lifetime = 50f;

    void Update()
    {
        if(this.transform.position.z <= -10)
        {
            Destruction();
        }

        void OnCollisionEnter(Collision coll)
        {
            if(coll.gameObject.name == "Air_Balloon")
            {
                Destruction();
            }
        }        
    }

    void Destruction()
    {
        Destroy(this.gameObject);
    }
}