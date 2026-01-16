using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[ExecuteInEditMode]
public class gridHex : MonoBehaviour
{

    public float cell_size_x = 1.5f;
    public float cell_size_y = 0.4375f;
    public Renderer rend;
    private float x, y, z, r;
    public gridHex[] nearTiles;
    public bool start = false;

    void Start()
    {
        x = 0f;
        y = 0f;
        z = 0f;
        rend = GetComponent<Renderer>();
        r = Mathf.Sqrt((cell_size_x * cell_size_x / 4) + (cell_size_y * cell_size_y));
    }

    void Update()
    {
        y = Mathf.Round(transform.position.y / cell_size_y) * cell_size_y;
        if(y/cell_size_y % 2 == 0) {
            x = Mathf.Round(transform.position.x / cell_size_x) * cell_size_x;
        }
        else
        {
            x = Mathf.Round(transform.position.x / cell_size_x - 0.5f) * cell_size_x + cell_size_x/2;
        }
        
        z = transform.position.z;
        
        transform.position = new Vector3(x, y, z);

        if(tag == "area1")
        {
            rend.material.color = Color.red;
        }
        else if(tag == "area2")
        {
            rend.material.color = new Color(1f, 0.5f, 0, 1f);
        }
        else if(tag == "area3")
        {
            rend.material.color = Color.yellow;
        }
        else if(tag == "area4")
        {
            rend.material.color = Color.green;
        }
        else if(tag == "area5")
        {
            rend.material.color = Color.blue;
        }

        //collect the tile object near this
        if (!start)
        {
            start = true;
            Collider2D[] nearColliders = Physics2D.OverlapCircleAll(new Vector3(x, y, z), r);
            int i = 0;
            int j = 0;
            nearTiles = new gridHex[nearColliders.Length-1];
            while (i < nearColliders.Length)
            {
                if (!(nearColliders[i].gameObject == this.gameObject))
                {
                    nearTiles[i - j] = nearColliders[i].GetComponent<gridHex>();
                }
                else
                {
                    j++;
                }
                i++;
            }
        }
    }
}
