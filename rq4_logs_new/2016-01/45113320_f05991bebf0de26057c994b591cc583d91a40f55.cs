using UnityEngine;
using System.Collections;

public class Move : IEnemyState
{

    public GameObject Ammo;
    Transform badboy;
    Vector3 directionShoot;
    GameObject[] target;
    Vector3 directionMove;
    public float shootRate = 0.5f;
    public float speed = 1;
    float lastShoot;
    public bool moveRight;

    public override void Run()
    {
        lastShoot += Time.deltaTime;
        if (lastShoot > shootRate)
        {
            Debug.Log("SHOOT");

            PullTheTrigger();
        }
        
        Moving();
        
    }

    public override void Stop()
    {
    }

    public override void Init(GameObject obj)
    {
        badboy = obj.GetComponent<Transform>();
        lastShoot = 0;
        target = GameObject.FindGameObjectsWithTag("Player");
        
        if (moveRight)
        {
            directionMove = new Vector3(1, 0, 0);
        }
        else
        {
            directionMove = new Vector3(-1, 0, 0);
        }
    }

    void Moving()
    {
        Vector3 pos = badboy.position;
       
        badboy.transform.position += directionMove*Time.deltaTime ;

    }

    void PullTheTrigger()
    {
        GameObject go;
        int rnd = Mathf.FloorToInt(Random.Range(0, target.Length - 1));
        //this.directionShoot = new Vector3(this.badboy.position.x + rnd, -5, 0);

        go = (GameObject)Instantiate(Ammo, this.badboy.position, this.badboy.rotation);

        Vector3 dir = target[rnd].transform.position - this.badboy.position;
        dir.Normalize();

        go.GetComponent<Fire>().Init(dir);
        lastShoot = 0;

    }
}