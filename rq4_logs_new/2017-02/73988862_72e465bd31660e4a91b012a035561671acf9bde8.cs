using UnityEngine;
using System.Collections;

public class LevelSelectFlechas : MonoBehaviour {
    private GameObject bob;
    Vector3 bobpos,tilepos;
    public string direccion;
    private bool flag=false, tmrflag=true,mover=false;
    // Use this for initialization
    public LayerMask map;
	void Start () {

        bob = GameObject.FindGameObjectWithTag("Bob");
        bobpos = bob.transform.position;
  

    }
   
    private void cooldown()
    {
        tmrflag = true;
    }

    private void arrangeTilePos()
    {
        tilepos = bobpos;
        switch (direccion)
        {
                
            case "Arriba":
                tilepos.x += 1f;
                tilepos.y += 0.5f;
                    
                break;
            case "Abajo":
                tilepos.x -= 1f;
                tilepos.y -= 0.5f;
                break;
            case "Derecha":
                tilepos.x += 1f;
                tilepos.y -= 0.5f;
                break;
            case "Izquierda":
                tilepos.x -=1f;
                tilepos.y += 0.5f;
                break;
            default:
                print("hola");
                break;
        }

        print(tilepos.x + "," + tilepos.y);

    }

    private void move(){

    }
    public void OnMouseDown()
    {
        if (tmrflag && Time.timeScale != 0)
        {
            flag = true;
            Invoke("cooldown", 0.25f);

            tmrflag = false;
        }
    }

    // Update is called once per frame
    void Update () {
        
        if (flag)
        {
            bobpos = bob.transform.position;
            arrangeTilePos();
            float radius = 0.1f;

            if (Physics2D.OverlapCircle(tilepos, radius,map))
            {
                print("go");
                mover = true;
            }
            else
            {
                print("no");
            }
            flag = false;
        }

        if ((bob.transform.position != tilepos) && mover)
        {
            bob.transform.position = Vector3.MoveTowards(bob.transform.position, tilepos, 5 * Time.deltaTime);

        }
        else
        {
            mover = false;
        }
    }
}