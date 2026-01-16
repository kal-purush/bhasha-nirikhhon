using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

public class Main2 : MonoBehaviour {

    private GameObject Red;
    private GameObject Yellow;
    private GameObject Green;
    private GameObject Blue;
    private Material RedMaterial;
    private Material BlueMaterial;
    private Material GreenMaterial;
    private Material YellowMaterial;
    List<int> pattern = new List<int>();
    int next;
    bool play_Back = false;
    void Start()
    {
        Red = GameObject.Find("/Cubes/Red");
        Yellow = GameObject.Find("/Cubes/Yellow");
        Green = GameObject.Find("/Cubes/Green");
        Blue = GameObject.Find("/Cubes/Blue");

        //RedMaterial = Resources.Load("Red Material", typeof(Material)) as Material;
        //BlueMaterial = Resources.Load("Blue Material", typeof(Material)) as Material;
        //GreenMaterial = Resources.Load("Green Material", typeof(Material)) as Material;
        //YellowMaterial = Resources.Load("Yellow Material", typeof(Material)) as Material;

    }

    // Update is called once per frame
    void Update()
    {
        //Red.GetComponent<Renderer>().material.color = Color.white;
        //Blue.GetComponent<Renderer>().material.color = Color.white;
        //Yellow.GetComponent<Renderer>().material.color = Color.white;
        //Green.GetComponent<Renderer>().material.color = Color.white;
        pattern.Add(0);
        pattern.Add(1);
        pattern.Add(2);
        pattern.Add(3);
        StartCoroutine(playBack());

    }

    IEnumerator playBack(){
        play_Back = true;

        for(int x = 0; x < pattern.Count;x++)
        {

            if (x == 0)
            {
                //pattern.Remove(x);
                Red.GetComponent<Renderer>().material.color = Color.white;
                yield return new WaitForSeconds(1f);
                Red.transform.GetComponent<Renderer>().material.color = Color.red;
                continue;
            }
            else if (x == 1)
            {
                //pattern.Remove(x);
                Green.GetComponent<Renderer>().material.color = Color.white;
                yield return new WaitForSeconds(1f);
                Green.GetComponent<Renderer>().material.color = Color.green;
                continue;
            }
            else if (x == 2)
            {
                //pattern.Remove(x);
                Blue.GetComponent<Renderer>().material.color = Color.white;
                yield return new WaitForSeconds(1f);
                Blue.GetComponent<Renderer>().material.color = Color.blue;
                continue;
            }
            else if (x == 3)
            {
                //pattern.Remove(x);
                Yellow.GetComponent<Renderer>().material.color = Color.white;
                yield return new WaitForSeconds(1f);
                Yellow.GetComponent<Renderer>().material.color = Color.yellow;
                continue;
            }
            yield return new WaitForSeconds(0.7f);
        }
        play_Back = false;
    }
        
    }