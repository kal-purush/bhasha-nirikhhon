using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Tree : MonoBehaviour
{
    enum State { healthy, unhealthy, burning };
    private State state;

    public float burningTime;       //countdown
    public float fireSpreadTime;    //countdown

    private float burningTimer;
    private float fireSpreadTimer;

    // Start is called before the first frame update
    void Start()
    {
        state = State.healthy;
    }

    // Update is called once per frame
    void Update()
    {
        if (state == State.burning) {
            // If countdown is done, destroy the tree
            burningTimer -= Time.deltaTime;
            if (burningTimer <= 0) {
                Destroy(gameObject);
                TreesController.instance.RemoveTree(gameObject);
            }

            // If countdown is done, spread the fire
            fireSpreadTimer -= Time.deltaTime;
            if (fireSpreadTimer <= 0)
            {
                fireSpreadTimer = fireSpreadTime;
                TreesController.instance.SpreadFire(transform.position);
            }

        }
    }

    public void StartBurning() {
        if (state == State.burning)
            return;
        state = State.burning;
        burningTimer = burningTime;
        fireSpreadTimer = fireSpreadTime;
        GetComponent<SpriteRenderer>().color = Color.red;
    }

    public void StopBurning()
    {
        state = State.healthy;
        GetComponent<SpriteRenderer>().color = Color.white;
    }
}