using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class IconGenerator : MonoBehaviour
{
    public List<GameObject> tokens;
    private List<GameObject> quadrants = new List<GameObject>();

    // Start is called before the first frame update
    void Start()
    {
        quadrants.Add(transform.Find("TL").gameObject);
        quadrants.Add(transform.Find("TC").gameObject);
        quadrants.Add(transform.Find("TR").gameObject);
        quadrants.Add(transform.Find("ML").gameObject);
        quadrants.Add(transform.Find("MC").gameObject);
        quadrants.Add(transform.Find("MR").gameObject);
        quadrants.Add(transform.Find("BL").gameObject);
        quadrants.Add(transform.Find("BC").gameObject);
        quadrants.Add(transform.Find("BR").gameObject);
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown("space"))
        {
            GenerateIcon();
        }
    }

    private void GenerateIcon()
    {
        for (int i=0; i<9; i++) 
        {
            GameObject token = tokens[Random.Range(0, 3)];
            var createImage = Instantiate(token) as GameObject;
            createImage.transform.SetParent(quadrants[i].transform, false);
        }
    }
}