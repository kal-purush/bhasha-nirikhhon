using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Utilities
{
    public static Quaternion GetGlobalRotation(Vector2 displacement){
        float angle = Mathf.Atan(displacement.x / displacement.y);
        float offset = displacement.y > 0 ? Mathf.PI : 0;
        float world_angle = (offset - angle) * Mathf.Rad2Deg;
        return Quaternion.Euler(new Vector3(0, 0, world_angle));       
    }
}