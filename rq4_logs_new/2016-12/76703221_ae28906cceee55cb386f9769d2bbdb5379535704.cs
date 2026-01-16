using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class OnCollision : MonoBehaviour {
    bool wormholed = false;
    Wormhole newWormhole;
    int numColliders = 0;
    void OnCollisionEnter(Collision collision)
    {
        if (GameManager.isLaunched)
        {
            Wormhole temp = collision.gameObject.GetComponent<Wormhole>();
            if (temp != null && temp.pairup != null)
            {

                if (!wormholed)
                {
                    wormholed = true;
                    numColliders = 0;
                    newWormhole = collision.gameObject.GetComponent<Wormhole>().pairup;

                    gameObject.GetComponent<Rocket>().setPosition(
                        newWormhole.gameObject.transform.position);
                }
                else
                {
                    if (collision.gameObject == newWormhole.gameObject)
                    {
                        numColliders += 1;
                    }
                }

                Debug.Log("ENTER:" + collision.gameObject.name + "    " + newWormhole.gameObject.name + "      " + numColliders.ToString());



            }
            else if (collision.gameObject.name == "End Planet")
            {
                GameManager.win();
            }
            else
            {
                GameManager.fail();
            }
        }
    }

    void OnCollisionExit(Collision collision)
    {

        if (wormholed)
        {
            Debug.Log("EXIT:" + collision.gameObject.name + "    " + newWormhole.gameObject.name + "      " + numColliders.ToString());

            if (collision.gameObject == newWormhole.gameObject)
            {
                numColliders -= 1;
                if (numColliders == 0)
                {
                    wormholed = false;
                    newWormhole = null;
                }
            }

        }
    }
}