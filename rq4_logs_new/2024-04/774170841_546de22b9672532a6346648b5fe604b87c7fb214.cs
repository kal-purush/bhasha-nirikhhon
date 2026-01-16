using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bullet : MonoBehaviour
{

    public float lifeTime = 3f;
    public Color goodColor = Color.green;

    private void Start()
    {
        DestroyBulletAfterDelay();
    }

    private void OnCollisionEnter(Collision collision)
    {
        /*Ray ray = playerCamera.ScreenPointToRay(Input.mousePosition);

        if (Physics.Raycast(ray, out RaycastHit hit, Mathf.Infinity, evilLayerMask))
        {
        }*/

        /*if (GetComponent<Collider>().CompareTag("EvilCharacter"))
            {
                GetComponent<Collider>().GetComponent<EvilCharacter>().Goodify(goodColor);
            }*/
        
        if (collision.collider.CompareTag("EvilCharacter"))
        {
            /*
            collision.collider.GetComponent<EvilCharacter>().Goodify(goodColor);
            */

            // Access the EvilCharacter component directly from the collided object
            EvilCharacter evilCharacter = collision.collider.GetComponent<EvilCharacter>();
            if (evilCharacter != null)
            {
                // Call the Goodify method of the EvilCharacter script to change its color
                evilCharacter.Goodify(goodColor);
            }
        }
    }

    private void DestroyBulletAfterDelay()
    {
        Destroy(gameObject,lifeTime);
    }
}