using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[ExecuteInEditMode]
public class grid : MonoBehaviour
{

    public float cell_size = 0.5f; // = larghezza/altezza delle celle
    public Renderer rend;
    public int posX, posY;
    private float x, y, z;

    void Start()
    {
        x = 0f;
        y = 0f;
        z = 0f;
        rend = GetComponent<Renderer>();

    }

    void Update()
    {
        x = Mathf.Round(transform.position.x / cell_size) * cell_size;
        y = Mathf.Round(transform.position.y / cell_size) * cell_size;
        z = transform.position.z;
        transform.position = new Vector3(x, y, z);

        posX = (int)Mathf.Round(x / 0.64f);
        posY = (int)Mathf.Round(y / -0.64f);
    }

    void OnMouseEnter()
    {
        rend.material.color = new Color(0.5f, 0.5f, 0.5f, 1f);
    }
    void OnMouseExit()
    {
        rend.material.color = new Color(1f, 1f, 1f, 1f);
    }

}
