using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerTeleportation : MonoBehaviour {
    bool isTeleporting;
    float energy;
    Player player;
    

	// Use this for initialization
	void Start () {
        isTeleporting = false;
        energy = 4f;
	}
	
	// Update is called once per frame
	void Update () {
        if (!isTeleporting)
        {
            RestoreEnergy();
        }
	}

    void RestoreEnergy()
    {
        if (energy < 4f)
        {
            energy += Time.deltaTime;
        }
    }

    void StartTeleport ()
    {
        if (energy != 0f)
        {
            isTeleporting = true;
        }
    }

    void StopTeleport ()
    {
        isTeleporting = false;
    }

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.collider.tag.Equals(""))
        {

        }
    }
}