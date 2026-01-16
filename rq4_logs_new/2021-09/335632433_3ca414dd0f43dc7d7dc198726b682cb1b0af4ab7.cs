using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class donguriRoll : MonoBehaviour
{
    // Start is called before the first frame update
    // Start is called before the first frame update
    public GameObject donguri;
    public GameObject wallcheckObj;
    Rigidbody rb;
    WallCheck wallcheck;
    Animator animator;
    private string rollStr = "isRoll";
    public bool isRoll = false;
    int keyCount = 0;
    public float speed;
    public float AddSpeed;
    public float MaxSpeed;
    float SetSpeed;
    void Start()
    {
        rb = GetComponent<Rigidbody>();
        wallcheck = wallcheckObj.GetComponent<WallCheck>();
        animator = donguri.GetComponent<Animator>();
        SetSpeed = speed;
    }

    // Update is called once per frame
    void Update()
    {
        bool isRollstart = animator.GetCurrentAnimatorStateInfo(0).IsName("Base Layer.Rolling");

        if (Input.GetKeyDown(KeyCode.R))
        {
            keyCount += 1;
            if (keyCount == 1)
            {
                isRoll = true;
                this.animator.SetBool(rollStr,true);
            }
        }

        if (keyCount == 1 && isRollstart)
        {
            rb.isKinematic = false;
            rb.constraints = RigidbodyConstraints.FreezeRotation;
            speed += AddSpeed;
            speed = Mathf.Clamp(speed, 0, MaxSpeed);
            rb.velocity = Vector3.zero;
            rb.velocity = transform.forward * speed;
        }

        if (isRoll)
        {
            if (wallcheck.touchWall == true)
            {
                this.animator.SetBool(rollStr, false);
                rb.constraints = RigidbodyConstraints.None;
                rb.velocity = Vector3.zero;
                rb.isKinematic = true;
                isRoll = false;
                speed = SetSpeed;
                keyCount = 0;
            }
        }
    }
}