using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerMove : MonoBehaviour {

    public Vector2 targetPosition;
    public float charMoveX;
    public float charMoveY;
    public Vector3 charMovement;
    public Vector2 charMovement2;
    public Vector3 charPosition;
    public Vector3 enemyMovement;
    public float enemyX;
    public float enemyY;
    public float speed = 2;
    public GameObject enemy;
    //public Animator anim
    
    // Update is called once per frame
    private void Start()
    {
        
        targetPosition = transform.position;
        charMovement = new Vector3(3, 0, 0);
    }
    void Update () {
        //targetPosition = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        //transform.position = targetPosition * Time.deltaTime;
        float step = 5 * Time.deltaTime; // calculate distance to move
        transform.position = Vector3.MoveTowards(transform.position, targetPosition, step);
        Controls();
        if (transform.position != charPosition)
        {
            if (enemy == null) { return; }
            else
            {
                enemy.transform.position = Vector3.MoveTowards(enemy.transform.position, transform.position, 1 * Time.deltaTime);
            }
        }
        
    }
    void Controls()
    {
        //if (Input.GetKey(KeyCode.RightArrow))
        //transform.position += Vector3.right * Time.deltaTime;
        if (Input.GetKeyDown(KeyCode.Mouse1))
        {
            float newX;
            float newY;
            targetPosition = Camera.main.ScreenToWorldPoint(Input.mousePosition);

            newX = targetPosition.x - charMovement.x;
            newY = targetPosition.y - charMovement.y;
            charPosition = new Vector3(targetPosition.x, targetPosition.y, 0);

            //enemyMovement = new Vector3
            //charMoveX = gameObject.transform.position.x + targetPosition.x;
            //charMoveY = gameObject.transform.position.y + targetPosition.y;
            //charMovement = new Vector3(charMoveX, charMoveY,0);

            //transform.position += Vector3.Lerp(transform.position, charMovement, 1*Time.deltaTime);
      
        }
    }
}