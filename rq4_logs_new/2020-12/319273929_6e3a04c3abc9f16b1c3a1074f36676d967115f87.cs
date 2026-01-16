using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShotScript : MonoBehaviour
{
    public JachManager manager;
    public float shotSpeed;
    private int id;
    private int player;
    private int dir;


    // Start is called before the first frame update
    
    public void setId(int i)
    {
        id = i;
    }

    public int getId()
    {
        return id;
    }

    public void setPlayer(int p)
    {
        player = p;
        dir = (player == 1) ? -1 : 1;
    }

    public int setPlayer()
    {
        return player;
    }

    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        transform.position +=  transform.right * Time.deltaTime * shotSpeed;
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        
    }
}