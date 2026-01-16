using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Backing : MonoBehaviour
{
    //public DeadManager deadManager;
    public float backingSpeed;
    private float timeToBack;
    private Vector2 posOrigen, posGoal;
    private bool backing;

    void Start()
    {
        posOrigen = transform.position;
    }

    /// <summary>
    /// Espera a que la barra baje a su posición inicial y
    /// sirve la bola.
    /// </summary>
    void Update()
    {

        if (backing)
        {
            timeToBack += backingSpeed * Time.deltaTime;
        }
        //Cuando llega la barra a su destino
        if (backing && transform.position.y <= 1)
        {
            backing = false;
            timeToBack = 0;
            GetComponent<PlayerController>().BackingPlayer(false);
            DeadManager.instancia.ServeBolaOnStart();
            
        }
    }
    private void FixedUpdate()
    {
        if (backing)
            transform.position = Vector2.Lerp(posGoal, posOrigen, timeToBack);

    }
    public void ActiveBacking()
    {
        backing = true;
        posGoal = transform.position;
        gameObject.GetComponent<PlayerController>().setCanUp(false);
    }
    public bool getBacking()
    {
        return backing;
    }
}