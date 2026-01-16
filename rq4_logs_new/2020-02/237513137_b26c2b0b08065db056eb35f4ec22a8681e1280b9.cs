using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GroundMovement : MonoBehaviour
{
    [SerializeField] private float MoveSpeed = 5.0f;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {

    }

    public void CalculateMovement()
    {
        float HorizontalInput = Input.GetAxis("Horizontal");
        float VerticalInput = Input.GetAxis("Vertical");

        Vector3 Direction = new Vector3(HorizontalInput, VerticalInput);

        if (Input.GetKey(KeyCode.LeftArrow))
        {
            transform.Translate(0.0f, 0.0f, MoveSpeed * Time.deltaTime);
        }
        else if (Input.GetKey(KeyCode.RightArrow))
        {
            transform.Translate(0.0f, 0.0f, -MoveSpeed * Time.deltaTime);
        }
        else if (Input.GetKey(KeyCode.UpArrow))
        {
            transform.Translate(MoveSpeed * Time.deltaTime, 0.0f, 0.0f);
        }
        else if (Input.GetKey(KeyCode.DownArrow))
        {
            transform.Translate(MoveSpeed * Time.deltaTime, 0.0f, 0.0f);
        }
        else if (Input.GetKey(KeyCode.Space))
        {
            transform.Translate(0.0f, MoveSpeed * Time.deltaTime, 0.0f);
        }
    }

    void TriggerDialogue(Collider other)
    {
        if (other.tag == "Owner1" && Input.GetKey(KeyCode.A))
        {
            //NPC 1 dialogue. Will add soon
        }

        if (other.tag == "Owner2" && Input.GetKey(KeyCode.A))
        {
            //NPC 2 dialogue. Will add soon
        }

        if (other.tag == "Owner3" && Input.GetKey(KeyCode.A))
        {
            //NPC 3 dialogue. Will add soon
        }

    }
}