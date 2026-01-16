using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MovableWalls : MonoBehaviour
{
    public int turnsTillmove;
    public int turnsWaited;
    public bool waiting;
    public bool wallDown;


    void Awake()
    {
        waiting = false;
        turnsWaited = 0;
    }


    public IEnumerator MoveWall()
    {
        if (!waiting)
        {
            turnsTillmove = Random.Range(1, 6);
            waiting = true;
            turnsWaited = 0;
            yield break;
        }
        
        else
        {
            turnsWaited++;
            if (turnsWaited >= turnsTillmove)
            {
                
                Vector3 dest = Vector3.zero;
                if (wallDown)
                {
                    RaycastHit phit;
                    int floormask = LayerMask.NameToLayer("floor");
                    if (Physics.Raycast(transform.position, Vector3.up, out phit, 10, ~(1 << floormask)))
                    {
                        yield break;
                    
                    }
                      
                    dest = transform.position + Vector3.up;
                    while (transform.position != dest)
                    {
                        yield return null;
                        transform.position = Vector3.Lerp(transform.position, dest, 5 * Time.deltaTime);
                    }
                }
                else
                {
                    dest = transform.position + Vector3.down;
                    while (transform.position != dest)
                    {
                        yield return null;
                        transform.position = Vector3.Lerp(transform.position, dest, 5 * Time.deltaTime);
                    }
                }
                wallDown = !wallDown;
                waiting = false;
            }
            
            yield break;

        }
    }
    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }
}