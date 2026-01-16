using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class BallController : MonoBehaviour
{
    public float speed;
    public Text countText;
    public float initialForceX,initialForceY;
    public Text lastText;
    public GameObject button;
    public GameObject northWall;
    private float initialY;

    private int count;
    private Rigidbody rb;

    void Start ()
    {
        rb = GetComponent<Rigidbody>();
        count=0;
        SetCountText();
        lastText.text="";
        initialY= this.transform.position.y;
        Vector3 movement = new Vector3 (initialForceX,initialForceY,0.0f);
        rb.AddForce (movement * speed);
        button.SetActive(false);
        transform.position = new Vector3(0,-3,northWall.transform.position.z);
        print(transform.position);
    }

    void Update(){
        checkLoser();
        transform.position = new Vector3(0,-3,northWall.transform.position.z);
    }

    void OnTriggerEnter(Collider other) 
    {
        if (other.gameObject.CompareTag ("brick"))
        {
            other.gameObject.SetActive (false);
            count=count+1;
            SetCountText();
        }
    }

    void SetCountText(){
        countText.text="Count: " + this.transform.position.y.ToString();
        if (count>=17){
            lastText.text="You won!";
            rb.constraints = RigidbodyConstraints.FreezeAll;
        }
    }

    void checkLoser(){
        if(this.transform.position.y < initialY-1){
            lastText.text="You lose!";
            rb.constraints = RigidbodyConstraints.FreezeAll;
            button.SetActive(true);
        }
    }
}