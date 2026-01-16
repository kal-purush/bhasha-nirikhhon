using UnityEngine;
using System.Collections;

public class Attack_Cone : MonoBehaviour {

    public TurretAI turretAI;

    public bool isLeft = false;
   
    

    void Awake()
    {
        turretAI = gameObject.GetComponentInParent<TurretAI>();

    }

 void onTriggerStay2D(Collider2D col)
    {
        Debug.Log(col.tag);
        if (col.CompareTag("Player"))
        {
            if (isLeft)
            {
                turretAI.Attack(false);
            }
            else
            {
                turretAI.Attack(true);
            }
        }
        
    }
}