using UnityEngine;
using System.Collections;

public class EnemyMovementController : MonoBehaviour {

    public float moveSpeed;
    private Rigidbody2D myRigidbody;

    private bool moving = false;
    private bool bewegtsich = false;

    public float timeBetweenMove = 30;
    private float timeBetweenMoveCounter = 30;
    public float timeToMove;
    private float timeToMoveCounter;

    //public Collider2D walkZone;
    //private Vector2 minWalkPoint;
    //private Vector2 maxWalkPoint;
    //private bool hasWalkZone;

	void Start () {

        //if (walkZone != null)
        //{
        //    minWalkPoint = walkZone.bounds.min;
        //    maxWalkPoint = walkZone.bounds.max;
        //    hasWalkZone = true;
        //}

        myRigidbody = GetComponent<Rigidbody2D>();

        timeBetweenMoveCounter = timeBetweenMove;
        timeToMoveCounter = timeToMove;

        if (timeToMoveCounter < 0)
        {
            timeBetweenMoveCounter = timeBetweenMove;
        }


    }

/*****************************************************************************************************************************************/

    void Update () {
        timeBetweenMoveCounter -= Time.deltaTime;
        bewegtsich = false;

        bleibstehn();

    }

/*****************************************************************************************************************************************/

    void bleibstehn()
    {
        int x = 10;
        int y = -10;
        int z = 1, z2 = 1;
        z = Random.Range(-1, 1);
        z2 = Random.Range(-1, 1);
        x = Random.Range(0, 10);
        y = Random.Range(0, 10);
        Debug.Log(z);
        if (z == 0)
        {
            z = 1;
        }
        if (z2 == 0)
        {
            z2 = 1;
        }
        Debug.Log(timeBetweenMoveCounter);
        if (!bewegtsich)
        {
           
            bewegtsich = true;
            if (timeBetweenMoveCounter < 0)
            {
                Invoke(MyConst.StopME, 5);
                timeBetweenMoveCounter = 8;
                Debug.Log(moving);
                timeToMoveCounter = timeToMove;
                if (x > 5 && y > 5)
                {
                    myRigidbody.velocity = new Vector2(z * moveSpeed, z2 * moveSpeed) / 2;

                    //if(hasWalkZone && transform.position.y > maxWalkPoint.y)      Prüfung bevor er in movement springt, wenn auserhalb der hitbox führt exakte bewegung mit funktion zurück ins zentrum aus,
                    //                                                              else normale bewegung
                    //{                                                             Wenn limit erreicht stoppt movement mit myRigidbody.velocity = Vector2.zero, so kann bei neuem update zurück bewegt werden,
                    //    myRigidbody.velocity = Vector2.zero;                      alles in 
                    //}

                }
                else
                { 
                    if (x > 5)
                        myRigidbody.velocity = new Vector2(z * moveSpeed, 0) / 2;
                    else
                        myRigidbody.velocity = new Vector2(0, z * moveSpeed) / 2;
                }
            }
        }
    }

/*****************************************************************************************************************************************/

    void StopME()
    {
        myRigidbody.velocity = Vector2.zero;
    }

}