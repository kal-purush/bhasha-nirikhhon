using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CharacterActions : MonoBehaviour {

    float speed, teleport;
    public GameObject prefab;


	void Start () {
        this.speed = 5f;
        this.teleport = 100f;
        prefab = Resources.Load("Prefabs/Projectile") as GameObject;
	}
	
    void Moving() {
        if (Input.GetKey(KeyCode.A))
            this.transform.position += new Vector3(-speed, 0, 0) * Time.deltaTime;
        else if (Input.GetKey(KeyCode.D))
            this.transform.position += new Vector3(speed, 0, 0) * Time.deltaTime;
        else if (Input.GetKey(KeyCode.W))
            this.transform.position += new Vector3(0, speed, 0) * Time.deltaTime;
        else if (Input.GetKey(KeyCode.S))
            this.transform.position += new Vector3(0, -speed, 0) * Time.deltaTime;
        if (Input.GetKey(KeyCode.W) && Input.GetKey(KeyCode.A))
            this.transform.position += ((new Vector3(-speed, speed , 0)).normalized) * speed * Time.deltaTime;
        if (Input.GetKey(KeyCode.W) && Input.GetKey(KeyCode.D))
            this.transform.position += ((new Vector3(speed, speed, 0)).normalized) * speed * Time.deltaTime;
        if (Input.GetKey(KeyCode.S) && Input.GetKey(KeyCode.D))
            this.transform.position += ((new Vector3(speed, -speed, 0)).normalized) * speed * Time.deltaTime;
        if (Input.GetKey(KeyCode.S) && Input.GetKey(KeyCode.A))
            this.transform.position += ((new Vector3(-speed, -speed, 0)).normalized) * speed * Time.deltaTime;
    }

    void Teleporting () {
        if (Input.GetKey(KeyCode.A) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(this.transform.position.x + (- teleport) , 0, 0) * Time.deltaTime;
        else if (Input.GetKey(KeyCode.D) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(this.transform.position.x + (+ teleport), 0, 0) * Time.deltaTime;
        else if (Input.GetKey(KeyCode.W) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(0, this.transform.position.y + (+ teleport), 0) * Time.deltaTime;
        else if (Input.GetKey(KeyCode.S) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(0, this.transform.position.y + (- teleport), 0) * Time.deltaTime;
        if (Input.GetKey(KeyCode.W) && Input.GetKey(KeyCode.A) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(this.transform.position.x + (- teleport), this.transform.position.y + ( + teleport), 0) * Time.deltaTime;
        if (Input.GetKey(KeyCode.W) && Input.GetKey(KeyCode.D) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(this.transform.position.x + (+ teleport), this.transform.position.y + (+ teleport), 0) * Time.deltaTime;
        if (Input.GetKey(KeyCode.S) && Input.GetKey(KeyCode.D) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(this.transform.position.x + (+ teleport), this.transform.position.y + (- teleport), 0) * Time.deltaTime;
        if (Input.GetKey(KeyCode.S) && Input.GetKey(KeyCode.A) && Input.GetKeyDown(KeyCode.LeftShift))
            this.transform.position += new Vector3(this.transform.position.x + (- teleport), this.transform.position.y + (- teleport), 0) * Time.deltaTime;
    }

    void Shooting() {
        if (Input.GetMouseButtonDown(0)) {
            GameObject projectile = Instantiate(prefab) as GameObject;
            projectile.transform.position = this.transform.position + Camera.main.transform.forward * 2;
            Rigidbody2D rb = projectile.GetComponent<Rigidbody2D> ();
            rb.velocity = Camera.main.transform.position * 100;
        }

    }

	void Update () {
        Moving ();
        Teleporting ();
        Shooting ();
	}
}