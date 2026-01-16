using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShootControls : MonoBehaviour
{
    public GameObject booletPrefab;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        //Shooting stuff
        if (Input.GetKeyDown(KeyCode.LeftArrow))
        {
            //GameObject boolet = Instantiate(Resources.Load<GameObject>("Prefabs/Boolet"));
            shootBoolet(Vector2.left);
        }
        if (Input.GetKeyDown(KeyCode.RightArrow))
        {
            shootBoolet(Vector2.right);
        }
        if (Input.GetKeyDown(KeyCode.UpArrow))
        {
            shootBoolet(Vector2.up);
        }
        if (Input.GetKeyDown(KeyCode.DownArrow))
        {
            shootBoolet(Vector2.down);
        }
    }
    private void shootBoolet(Vector2 direction)
    {
        GameObject boolet = Instantiate(booletPrefab, transform.position, Quaternion.identity);
        ProjectileLogic projectileLogic = boolet.GetComponent<ProjectileLogic>();
        projectileLogic.Direction(direction);

    }
}