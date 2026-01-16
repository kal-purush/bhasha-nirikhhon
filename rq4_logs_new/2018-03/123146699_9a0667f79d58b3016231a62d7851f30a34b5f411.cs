using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Globalization;


public class EnemyBehaviour : MonoBehaviour
{

    [SerializeField]
    string[] Points;

    [SerializeField]
    float speed;

    [SerializeField]
    float searchModeSpeed;

    [SerializeField]
    int typeOfPath;

    EnemyVision scriptVision;

    private int current = 0;
    private Vector3 target;
    private float[] X = new float[10];
    private float[] Y = new float[10];
    private float[] Z = new float[10];
    private float turnSpeed = 5f;
    private new Rigidbody rigidbody;
    
    
    private bool isVisible;
    private bool playerEscaped;
    private System.Random ran = new System.Random();
    private int turn;
    private bool turning = false;
    private int turnCount = 0;



    void Start()
    {
        rigidbody = GetComponent<Rigidbody>();
        GetXYZ();
    }

    void GetXYZ()
    {

        int j = 0;

        foreach (string point in Points)
        {
            string[] values = point.Split(';');

            X[j] = float.Parse(values[0], CultureInfo.InvariantCulture);
            Y[j] = float.Parse(values[1], CultureInfo.InvariantCulture);
            Z[j] = float.Parse(values[2], CultureInfo.InvariantCulture);
            j++;

        }

        target = new Vector3(X[0], Y[0], Z[0]);
        transform.LookAt(target);

    }




    // Update is called once per frame
    void Update()
    {
        
        scriptVision = GetComponent<EnemyVision>();
        List<Transform> visible = scriptVision.visibleTargets;
        

        if (visible.Count == 0)
            isVisible = false;
        else isVisible = true;


        //Quaternion newRotation = Quaternion.AngleAxis(90, Vector3.up);
        //transform.rotation = Quaternion.Slerp(transform.rotation, newRotation, .05f);



        if (isVisible)
        {
            Attack();
            playerEscaped = true;
            turning = false;
            turnCount = 0;
        }
        else if (playerEscaped)
        {
            if (turnCount < 210)
            {
                Rotate();
                turnCount++;
            }
            else
            { ContinuePatroling(typeOfPath);
            playerEscaped = false;}
            //ReturnToPath(typeOfPath);
            
        }
        else 
        {
            ContinuePatroling(typeOfPath);
            
        }





    }

    public void Attack()
    {

    }


    public void Rotate()
    {
        if (!turning)
        {
            turn = ran.Next(0, 2);
            turning = true;
        }
            Vector3 angles;
            angles = transform.rotation.eulerAngles;
            switch (turn)
            {

                case 0:
                    angles.y -= Time.deltaTime * searchModeSpeed;
                    
                    break;
                case 1:
                    angles.y += Time.deltaTime * searchModeSpeed;
                    break;
            }
            transform.rotation = Quaternion.Euler(angles);
        }
    



    public void ReturnToPath(int typeOfPath)
    {

    }

    public void ContinuePatroling(int typeOfPath)
    {
        if (current == Points.Length)
        {
            current = 0;
            target = new Vector3(X[current], Y[current], Z[current]);
        }

        if (transform.position != target)
        {
            Vector3 direction = target - transform.position;
            Quaternion rotation = Quaternion.LookRotation(direction);
            transform.rotation = Quaternion.Lerp(transform.rotation, rotation, turnSpeed * Time.deltaTime);

            Vector3 move = Vector3.MoveTowards(transform.position, target, speed * Time.deltaTime);
            GetComponent<Rigidbody>().MovePosition(move);
        }
        else
        {
            current++;
            target = new Vector3(X[current], Y[current], Z[current]);
        }
    }


}