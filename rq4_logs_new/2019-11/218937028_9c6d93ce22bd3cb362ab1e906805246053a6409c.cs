
using System.Collections;
using System.Collections.Generic;
using UnityEngine;



public class CreatController : MonoBehaviour
{
     private GameObject head;
     private GameObject acc;
     private GameObject head_position;

     private GameObject head_position_top;

     private GameObject body;
     private GameObject clothJacket;
     private GameObject clothShadow;
     private GameObject bag;
      public Color altColor;
     private Color hsvColor;
     private float hue = 0, value = 1;
     public int year = 0;
     public float saturation = 0.5f;
     public int field =1;
   
   public GameObject[] feildObject = new GameObject[7];

   public GameObject[] item = new GameObject[10];
   
   private GameObject[] itemPosition = new GameObject[10];

    private GameObject targetHead;
    private Color jacketColor;
   private GameObject parentPosition;

   public int testNum = 3;

   public Animator creatMotion;
 


    
    // Start is called before the first frame update
    void Start()
    {
        head = GameObject.FindGameObjectWithTag("head");
        head_position = GameObject.FindGameObjectWithTag("HeadPosition");
        body = GameObject.FindGameObjectWithTag("body");
        clothJacket = GameObject.FindGameObjectWithTag("cloth_jacket");
        clothShadow = GameObject.FindGameObjectWithTag("cloth_shadow");
        itemPosition[0] = GameObject.FindGameObjectWithTag("backpack_position");
        itemPosition[1] = GameObject.FindGameObjectWithTag("backpack_back_position");
        itemPosition[2] = GameObject.FindGameObjectWithTag("neckbag_position");
        itemPosition[3] = GameObject.FindGameObjectWithTag("crosshipback_position");
        itemPosition[4] = GameObject.FindGameObjectWithTag("carrier_position");
        itemPosition[5] = GameObject.FindGameObjectWithTag("hipback_position");
        itemPosition[6] = GameObject.FindGameObjectWithTag("laptopcase_position");
        itemPosition[7] = GameObject.FindGameObjectWithTag("suitcase_position");
        itemPosition[8] = GameObject.FindGameObjectWithTag("thingbag_position");
        creatMotion = GetComponent<Animator>();
        itemPosition[9] = GameObject.FindGameObjectWithTag("totebag_position");

         hue = 352f / 360f;
         targetHead = feildObject[field-1];
         altColor =  Color.HSVToRGB(hue, saturation, value);

        head.GetComponent<Renderer>().material.color = altColor;
        clothJacket.GetComponent<Renderer>().material.color = altColor;
        clothShadow.GetComponent<Renderer>().material.color = altColor;






         //Call Example to set all color values to zero.
         //Get the renderer of the object so we can access the color
         
         //Set the initial color (0f,0f,0f,0f)
         

        
    }
    public void createAcc(int index){

        if (index == 11)
        {
            Destroy(acc);
        }
        else
        { 
            if (acc)
            {
                Destroy(acc);
            }
            acc = Instantiate(item[index], itemPosition[index].transform.position, itemPosition[index].transform.rotation);
            acc.transform.parent = itemPosition[index].transform;

            acc.GetComponent<Renderer>().material.color = altColor;
        }
    }
    
    public void changeJacketColor(){
         jacketColor =  Color.HSVToRGB(hue, saturation, value);
         clothJacket.GetComponent<Renderer>().material.color = jacketColor;
    }
    public void create(float index){
                switch (index)
         {
             case 1 :
                hue = 352f / 360f;
                targetHead = feildObject[field-1];
                break;
             case 2 :
                hue = 26f / 360f;
                targetHead = feildObject[field-1];
                break;
             case 3 :
                hue = 50f / 360f;
                targetHead = feildObject[field-1];
                break;
              case 4 :
                hue = 97f / 360f;
                value = 0.902f;
                targetHead = feildObject[field-1];
                break;
             case 5 :
                hue = 188f / 360f;
                targetHead = feildObject[field-1];
                value = 0.902f;
                break;

             case 6 :
                hue = 224f / 360f;
                targetHead = feildObject[field-1];
                break;

             case 7 :
                hue = 265f / 360f;
                targetHead = feildObject[field-1];
                break;           
             default:
                break;
         }
         altColor =  Color.HSVToRGB(hue, 1, value);

         head.GetComponent<Renderer>().material.color = altColor;
         clothJacket.GetComponent<Renderer>().material.color = altColor;
         clothShadow.GetComponent<Renderer>().material.color = altColor;
         changeHead(targetHead);

    }

    void rendAcc(int index){
       
    }


    // Update is called once per frame
    void Update()
    {
        if(Input.GetKey ("1")){
            changeHead(targetHead);
        }
             {
         if (Input.GetKeyDown (KeyCode.G)){  
             //Alter the color       
                // altColor.saturation -= 0.5f;
             //Assign the changed color to the material.
            saturation -= 0.1f;
            altColor =  Color.HSVToRGB(hue, saturation, value);

            gameObject.GetComponent<Renderer>().material.color = altColor;
         }
     }         
        
    }

    void changeHead( GameObject feild){
      if(head){
         Destroy(head);
      }
      
        head = Instantiate(feild,head_position.transform.position,head_position.transform.rotation);
        head.transform.parent = head_position.transform.parent;
        head.GetComponent<Renderer>().material.color = altColor;


    }
}