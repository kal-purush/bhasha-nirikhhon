using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class RandomIncorrectOrder : MonoBehaviour
{
    public Button Order1;
    public Button Order2;
    public Button Order3;
    public Button Order4;


    public string[] RandomFoods = new string[]
   {
        "Single Cheeseburger",
        "Double Cheeseburger",
        "Steak",
        "Ribs",
        "Taco",
        "Noodles",
        "Pizza",
        "Hot Wings",
        "Chicken Piece",
        "Sushi 1",
        "Sushi 2",
        "Sushi 3",
        "Fried Fish",
        "Fried Calamari",
        "Fried Prawns",
        "Salad",
        "Chips",
        "Onion Rings",
        "Rice",
        "Doughnuts",
        "Ice Cream",
        "Cupcakes",
        "Waffles",
        "Milkshake",
        "Water",
        "Juice",
        "Alchol",
        "Cold Drink",

        //EARTH PLANET
        "Human Food",

        //JUNGLE PLANET
        "Bugs",

        //LAVA PLANET
        "Lava Drink",

        //PILLOW PLANET
        "Warm Milk",

        //THE SUN
        "Sun food",

        
        //ICE PLANET 
        "Snow cone",

        
        //CAT PLANET
        "Cat food",

        //ROBOT PLANET
        "Nuts and Bolts",


        //LSD PLANET
        "Space Shrooms",

        //GAS PLANET
        "Helium",

        //DISCO PLANET
        "Energy Drink",

        
        // WEED PLANET
        "Joint",

        //BOUGIE PLANET
        "Diamond Caviar",


        //LOVECRAFT PLANET
        "THE VOID",
   };


    public string PickRandomFood1, PickRandomFood2, PickRandomFood3, PickRandomFood4, PickRandomFood5, PickRandomFood6, PickRandomFood7, PickRandomFood8, PickRandomFood9, PickRandomFood10, PickRandomFood11, PickRandomFood12;

    public GameObject OrderPanel;
    // Start is called before the first frame update
    void Start()
    {
        OrderPanel.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {

    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            Debug.Log("COLLITION!!!");
            OrderPanel.SetActive(true);
            PickRandomFoodOrder();
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            OrderPanel.SetActive(false);
        }
    }

    public void PickRandomFoodOrder()
    {
        for (int counrt = 0; counrt < 4; counrt++)
        {
            if (counrt == 0)
            {
                FourRandomFoods();

                Order1.gameObject.GetComponentInChildren<Text>().text = PickRandomFood1 + "," + PickRandomFood2 + "," + PickRandomFood3;

            }
            if (counrt == 1)
            {
;
                FourRandomFoods();
                Order2.gameObject.GetComponentInChildren<Text>().text = PickRandomFood4 + "," + PickRandomFood5 + "," + PickRandomFood6;
            }
            if (counrt == 2)
            {

                FourRandomFoods();
                Order3.gameObject.GetComponentInChildren<Text>().text = PickRandomFood7 + "," + PickRandomFood8 + "," + PickRandomFood9;
            }
            if (counrt == 3)
            {
                FourRandomFoods();
                Order4.gameObject.GetComponentInChildren<Text>().text = PickRandomFood10 + "," + PickRandomFood11 + "," + PickRandomFood12;
            }
        }
       
    }

    public void FourRandomFoods()
    {
        System.Random random1 = new System.Random();
        int useFoods1 = Random.Range(0, RandomFoods.Length);

        PickRandomFood1 = RandomFoods[useFoods1];
        Debug.Log("This is" + useFoods1);


        System.Random random2 = new System.Random();
        int useFoods2 = Random.Range(0, RandomFoods.Length);

        PickRandomFood2 = RandomFoods[useFoods2];
        Debug.Log("This is" + useFoods2);

        System.Random random3 = new System.Random();
        int useFoods3 = Random.Range(0, RandomFoods.Length);

        PickRandomFood3 = RandomFoods[useFoods3];
        Debug.Log("This is" + useFoods3);

        System.Random random4 = new System.Random();
        int useFoods4 = Random.Range(0, RandomFoods.Length);

        PickRandomFood4 = RandomFoods[useFoods4];
        Debug.Log("This is" + useFoods4);


        System.Random random5 = new System.Random();
        int useFoods5 = Random.Range(0, RandomFoods.Length);

        PickRandomFood5 = RandomFoods[useFoods5];
        Debug.Log("This is" + useFoods5);


        System.Random random6 = new System.Random();
        int useFoods6 = Random.Range(0, RandomFoods.Length);

        PickRandomFood6 = RandomFoods[useFoods6];
        Debug.Log("This is" + useFoods6);


        System.Random random7 = new System.Random();
        int useFoods7 = Random.Range(0, RandomFoods.Length);

        PickRandomFood7 = RandomFoods[useFoods7];
        Debug.Log("This is" + useFoods7);



        System.Random random8 = new System.Random();
        int useFoods8 = Random.Range(0, RandomFoods.Length);

        PickRandomFood8 = RandomFoods[useFoods8];
        Debug.Log("This is" + useFoods8);



        System.Random random9 = new System.Random();
        int useFoods9 = Random.Range(0, RandomFoods.Length);

        PickRandomFood9 = RandomFoods[useFoods9];
        Debug.Log("This is" + useFoods9);



        System.Random random10 = new System.Random();
        int useFoods10 = Random.Range(0, RandomFoods.Length);

        PickRandomFood10 = RandomFoods[useFoods10];
        Debug.Log("This is" + useFoods10);




        System.Random random11= new System.Random();
        int useFoods11 = Random.Range(0, RandomFoods.Length);

        PickRandomFood11 = RandomFoods[useFoods11];
        Debug.Log("This is" + useFoods11);



        System.Random random12 = new System.Random();
        int useFoods12 = Random.Range(0, RandomFoods.Length);

        PickRandomFood12 = RandomFoods[useFoods12];
        Debug.Log("This is" + useFoods12);
    }

}