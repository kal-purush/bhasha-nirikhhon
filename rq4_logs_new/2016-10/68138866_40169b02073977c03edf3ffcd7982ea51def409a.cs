using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using UnityEngine.SceneManagement;
public class Main2 : MonoBehaviour
{
    private static int counting = 1;
    private GameObject Red;
    private GameObject Yellow;
    private GameObject Green;
    private GameObject Blue;
    public Editor edit;
    /*private Material RedMaterial;
    private Material BlueMaterial;
    private Material GreenMaterial;
    private Material YellowMaterial;*/
    static List<int> pattern = new List<int>();
    int next;
    bool play_Back = false;
    int onInList = 0;
    int r;
    private string NewBeatThatProto;
    static int count = 0;

    void Start()
    {
        Red = GameObject.Find("/Cubes/Red");
        Yellow = GameObject.Find("/Cubes/Yellow");
        Green = GameObject.Find("/Cubes/Green");
        Blue = GameObject.Find("/Cubes/Blue");
        //for(count; count < counting; count++)
        //{
            r = Random.Range(0, 4);
            pattern.Add(r);

        //}
        
        //Debug.Log(pattern[0]);
        StartCoroutine(playBack());


        //RedMaterial = Resources.Load("Red Material", typeof(Material)) as Material;
        //BlueMaterial = Resources.Load("Blue Material", typeof(Material)) as Material;
        //GreenMaterial = Resources.Load("Green Material", typeof(Material)) as Material;
        //YellowMaterial = Resources.Load("Yellow Material", typeof(Material)) as Material;

    }
    public static Main2 Instance { get; private set; }

    // Update is called once per frame
    void Update()
    {
        //Red.GetComponent<Renderer>().material.color = Color.white;
        //Blue.GetComponent<Renderer>().material.color = Color.white;
        //Yellow.GetComponent<Renderer>().material.color = Color.white;
        //Green.GetComponent<Renderer>().material.color = Color.white;  
        //StopCoroutine(playBack());
        
    }



    IEnumerator playBack()
    {
        Debug.Log(pattern.Count + " start ");
        play_Back = true;
        if (pattern != null)
        {
            for (int x = 0; x < pattern.Count; x++)
            {

                if (pattern[x] == 0)
                {
                    //pattern.Remove(x);
                    Red.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Red.transform.GetComponent<Renderer>().material.color = Color.red;
                    yield return new WaitForSeconds(0.6f);
                    continue;
                }
                else if (pattern[x] == 1)
                {
                    //pattern.Remove(x);
                    Green.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Green.GetComponent<Renderer>().material.color = Color.green;
                    yield return new WaitForSeconds(0.6f);
                    continue;
                }
                else if (pattern[x] == 2)
                {
                    //pattern.Remove(x);
                    Blue.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Blue.GetComponent<Renderer>().material.color = Color.blue;
                    yield return new WaitForSeconds(0.6f);
                    continue;
                }
                else if (pattern[x] == 3)
                {
                    //pattern.Remove(x);
                    Yellow.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Yellow.GetComponent<Renderer>().material.color = Color.yellow;
                    yield return new WaitForSeconds(0.6f);
                    continue;
                }
                yield return new WaitForSeconds(0.7f);
            }
        }
        play_Back = false;

    }

    public void testCorrect(int x)
    {
        if (play_Back) { return; }
        if (pattern[onInList]==x)
        {
            Debug.Log(pattern.Count + "            int " + x);
            onInList++;
            Debug.Log(pattern.Count +"    vs    oninlist: "    + onInList);
           if(onInList == pattern.Count) {
                counting++;
                SceneManager.LoadScene("NewBeatThatProto");
           }
        }
        else
        {
            counting = 1;
            pattern = new List<int>();
            SceneManager.LoadScene("NewBeatThatProto");
        }

    }
}
        
    