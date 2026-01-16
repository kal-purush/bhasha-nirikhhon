using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Connector : Health {


    private CircleCollider2D deathRadius = null;
    private bool connected = false;
    public float maxTimeDead = 5.0f;
    private float timeDead = 0.0f;
    private bool connectedToTower = false;

    private ArrayList connectedObjects;

    private void Awake()
    {
        connectedObjects = new ArrayList();
    }


    // Override player die method
    public override void Die() {
        if (!deathRadius) {
            gameObject.AddComponent<CircleCollider2D>();
            deathRadius = gameObject.GetComponent<CircleCollider2D>();
            deathRadius.isTrigger = true;
            //deathRadius.transform.localScale = new Vector2(5, 5);
            deathRadius.radius = 5;
        }
    }

    public override void Respawn() {
        connected = false;
    }

    public bool isConnected() {
        return connected;
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if(collision.gameObject.tag == "Tower") {
            connected = true;
        }
        if(collision.gameObject.tag == "Connector") {
            if(collision.gameObject.GetComponent<Connector>().isConnected()){
                connected = true;
            }
        }
    }

    private void OnTriggerStay2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Tower")
        {
            connected = true;
            connectedToTower = true;
            return;
        }
        connectedObjects.Add(collision.gameObject);


    }




    public override void Update()
    {
        base.Update();

        if(!isAlive) {
            //Check for connection
            bool hasConnection = false;
            if (!connectedToTower)
            {
                if (connectedObjects.Count > 0)
                {
                    Debug.Log("Connected to chain");
                    foreach (GameObject connectedObject in connectedObjects)
                    {
                        if (connectedObject.GetComponent<Connector>().isConnected())
                        {
                            hasConnection = true;
                            Debug.Log("Chain has power");
                            gameObject.GetComponent<Rigidbody2D>().angularVelocity = 80.0f;

                        }

                    }
                    if(!hasConnection) {
                        Debug.Log("CONNECTION HAS BEEN LOST");
                    }
                    connected = hasConnection;
                    connectedObjects.Clear();
                }
            }
            else {
                Debug.Log("Connected to a tower");
                gameObject.GetComponent<Rigidbody2D>().angularVelocity = 80.0f;

                connectedToTower = false;
            }

            timeDead += Time.deltaTime;

            if(timeDead >= maxTimeDead) {
                Destroy(gameObject);
            }
        }
        
    }



}