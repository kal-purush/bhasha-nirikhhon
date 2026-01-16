using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PowerUp : MonoBehaviour
{
    [SerializeField]
    private float _speed = 5.0f;

    // Update is called once per frame
    void Update()
    {
        transform.Translate(Vector3.down * _speed *Time.deltaTime);
    }

    private void OnTriggerEnter2D(Collider2D other)
    {
        //Access to class player
        Player player = other.GetComponent<Player>();

        if (other.tag == "Player")
        {
            if (player != null)
            {
                //Enable tripleshoot
                player.canTripleShoot = true;
            }

            //Destroy
            Destroy(this.gameObject);
        }
    }
}