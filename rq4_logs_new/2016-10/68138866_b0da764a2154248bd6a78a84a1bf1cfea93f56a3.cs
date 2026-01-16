using UnityEngine;
using System.Collections.Generic;
using System.Threading;


public class Flashanimation : MonoBehaviour {

    // Use this for initialization
    private GameObject Red;
    private GameObject Yellow;
    private GameObject Green;
    private GameObject Blue;
    /*private Material RedMaterial;
    private Material BlueMaterial;
    private Material GreenMaterial;
    private Material YellowMaterial;
    List<int> pattern = new List<int>();
    int next;
    bool play_Back = false;*/
	void Start () {
        Red = GameObject.Find("/Cube/Red");
        Yellow = GameObject.Find("/Cube/Yellow");
        Green = GameObject.Find("/Cube/Green");
        Blue = GameObject.Find("/Cube/Blue");

        /*RedMaterial = Resources.Load("Red Material", typeof(Material)) as Material;
        BlueMaterial = Resources.Load("Blue Material", typeof(Material)) as Material;
        GreenMaterial = Resources.Load("Green Material", typeof(Material)) as Material;
        YellowMaterial = Resources.Load("Yellow Material", typeof(Material)) as Material;*/

        

       // playBack();

    }
	
	// Update is called once per frame
	void Update () {
        Red.GetComponent<Renderer>().material.color = Color.white;
        /*pattern.Add(1);
        pattern.Add(2);
        pattern.Add(3);
        pattern.Add(4);
        new Thread(playBack).Start();*/

    }

    /*void playBack(){
        play_Back = true;
        foreach (int color in pattern)
        {
            switch (color)
            {
                case 0:
                    Red.transform.GetComponent<Renderer>().material.color = Color.white;
                    Thread.Sleep(400);
                    Red.transform.GetComponent<Renderer>().material = RedMaterial;
                    break;
                case 1:
                    Green.transform.GetComponent<Renderer>().material.color = Color.white;
                    Thread.Sleep(400);
                    Green.transform.GetComponent<Renderer>().material = GreenMaterial;
                    break;
                case 2:
                    Blue.transform.GetComponent<Renderer>().material.color = Color.white;
                    Thread.Sleep(400);
                    Blue.transform.GetComponent<Renderer>().material = BlueMaterial;
                    break;
                case 3:
                    Yellow.transform.GetComponent<Renderer>().material.color = Color.white;
                    Thread.Sleep(400);
                    Yellow.transform.GetComponent<Renderer>().material = YellowMaterial;
                    break;
            }
            Thread.Sleep(600);
        }
        play_Back = false;
    }*/
    
}