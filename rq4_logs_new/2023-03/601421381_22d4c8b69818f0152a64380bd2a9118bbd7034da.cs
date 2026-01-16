//Destroy rigid body objects that colide with this object

using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Destroyer : MonoBehaviour
{

void OnCollisionEnter2D(Collision2D col)
{
    Destroy(col.gameObject);
}
        
}