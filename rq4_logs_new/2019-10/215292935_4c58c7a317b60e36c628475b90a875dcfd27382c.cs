using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
    public float speed = 5f;
    private float horizontalInput;
    private float verticalInput;

    // Kann über Unity Editor eingefügt werden. Projektil sollte ein Skript haben dass dafür sorgt, dass sich das Projektlich in eine FESTE Richtung bewegt
    public GameObject projectile;

    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        horizontalInput = Input.GetAxis("Horizontal");
        verticalInput = Input.GetAxis("Vertical");


        // Ermöglicht Playermovement durch WASD oder die Pfeiltasten
        if (Input.GetKey(KeyCode.W) || Input.GetKey(KeyCode.UpArrow))
        {
            transform.Translate(Vector3.forward * verticalInput * Time.deltaTime * speed);
        }

        if (Input.GetKey(KeyCode.S) || Input.GetKey(KeyCode.RightArrow))
        {
            transform.Translate(Vector3.forward * verticalInput * Time.deltaTime * speed);
        }

        if (Input.GetKey(KeyCode.A) || Input.GetKey(KeyCode.LeftArrow))
        {
            transform.Translate(Vector3.right * horizontalInput * Time.deltaTime * speed);
        }

        if (Input.GetKey(KeyCode.D) || Input.GetKey(KeyCode.DownArrow))
        {
            transform.Translate(Vector3.right * horizontalInput * Time.deltaTime * speed);
        }


        // Wenn Player Leertaste drückt wird ein Projektil erstellt.
        if (Input.GetKeyDown(KeyCode.Space))
        {
            Instantiate(projectile, transform.position, projectile.transform.rotation);
        }

    }
}