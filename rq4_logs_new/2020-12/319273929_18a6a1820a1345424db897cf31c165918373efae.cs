using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BodyScript : MonoBehaviour
{
    public float moveForce;
    public float spawnDistance;
    public GameObject linkPrefab;
    public List<GameObject> links;
    private GameObject playerParent;
    Rigidbody2D headBody;

    // Start is called before the first frame update
    void Start()
    {
        links = new List<GameObject>();
        moveForce = 30f;
        headBody = GetComponent<Rigidbody2D>();
        playerParent = transform.parent.gameObject;
        for (int i = 0; i < 5; i++)
        {
            addLink();
        }
    }

    void addLink()
    {
        Vector2 lastPos;
        Vector2 spawnPos;
        GameObject last;
        Quaternion spawnRot;
        if (links.Count == 0)
        {
            last = this.gameObject;
            if (headBody.velocity.magnitude > 0)
            {
                spawnPos = headBody.velocity;
                spawnPos.Normalize();
            }
            else spawnPos = Vector2.right;
            spawnPos = new Vector2(transform.position.x, transform.position.y) - (spawnPos * spawnDistance);
            spawnRot = transform.rotation;
        }
        else
        {
            last = links[links.Count - 1];
            lastPos = last.transform.position;
            Vector2 beforeLastPos;
            if (links.Count > 1) beforeLastPos = links[links.Count - 2].transform.position;
            else beforeLastPos = transform.position;
            spawnPos = 2* lastPos - beforeLastPos;
            spawnRot = links[links.Count - 1].transform.rotation;
        }
        GameObject newLink = Instantiate(linkPrefab, spawnPos, spawnRot);
        newLink.GetComponent<HingeJoint2D>().enabled = false;
        last.GetComponent<HingeJoint2D>().enabled = true;
        last.GetComponent<HingeJoint2D>().connectedBody = newLink.GetComponent<Rigidbody2D>();
        newLink.transform.parent = playerParent.transform;
        links.Add(newLink);
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKey(KeyCode.UpArrow))
        {
            headBody.AddForce(Vector2.up * moveForce);
        }
        if (Input.GetKey(KeyCode.RightArrow))
        {
            headBody.AddForce(Vector2.right * moveForce);
        }
        if (Input.GetKey(KeyCode.DownArrow))
        {
            headBody.AddForce(Vector2.down * moveForce);
        }
        if (Input.GetKey(KeyCode.LeftArrow))
        {
            headBody.AddForce(Vector2.left * moveForce);
        }
        if (Input.GetKeyDown(KeyCode.Space))
        {
            addLink();
        }
    }

    void checkFoodPickup(GameObject link)
    {
        List<Vector2> vertices = new List<Vector2>();
        vertices.Add(transform.position);
        int i = 1;
        while(links[i - 1] != link)
        {
            vertices.Add(links[i].transform.position);
            i++;
        }
        int[] triangles = new int[vertices.Count * 3];
        for (i = 0; i < vertices.Count - 1; i++)
        {
            triangles[i * 3] = i + 2;
            triangles[i * 3 + 1] = 0;
            triangles[i * 3 + 2] = i + 1;
        }
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (collision.gameObject.tag == "link")
        {

        }
    }
}