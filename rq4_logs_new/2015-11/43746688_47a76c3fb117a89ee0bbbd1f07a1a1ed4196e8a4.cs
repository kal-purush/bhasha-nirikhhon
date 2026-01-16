using UnityEngine;
using System.Collections;

public class RockPunch : PrekazkaBase {

    private bool isRekt = false;

    void Update()
    {
        if (isRekt)
        {
            this.transform.position += Vector3.right * 2;
        }
        if (this.transform.position.z > 2000) Destroy(this);
    }

    public override void Kill()
    {
        isRekt = true;
    }
}