using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BreakableWall : MonoBehaviour
{
    public int numberOfHitRequiredToBreakWall;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        var _skin = collision.GetComponent<EnemySkin>();
        if (_skin)
        {
            var _groundChargingEnemy = _skin.GetComponentInParent<Enemy1>();
            if (_groundChargingEnemy)
            {
                if (_groundChargingEnemy.isGroundCharging)
                {
                    --numberOfHitRequiredToBreakWall;
                    if(numberOfHitRequiredToBreakWall <= 0)
                    {
                        BreakWall();
                    }
                }
            }
        }
    }

    public void BreakWall()
    {
        Debug.Log("Break wall");
    }

}
