using UnityEngine;
using System.Collections;

public class Ingredients : MonoBehaviour {
	public int numI= 4; //four ingredients that we have listed so far

    public string name;
	public float cookTime;
    public Vector2 flavor;

    public bool isCooking;

	// Use this for initialization
	void Start () {
	}

    public void cookSelf(string tag)
    {
        if (tag == "cut")
        {
            if (name == "Almond")
            {
                Countdown("newFoodName");
            }
            if (name == "Saffron")
            {
                
            }
            if (name== "")
            {

            }
            if (name== "Water")
            {

            }
        }

        //add more functions for the various tags
        
        if (tag == "fry")
        {
            if (name == "Almond")
            {
                Countdown("newFoodName");
            }
            if (name == "Saffron")
            {

            }
            if (name == "")
            {

            }
            if (name == "Water")
            {

            }
        }
        if (tag == "bake")
        {
            if (name == "Almond")
            {
                Countdown("newFoodName");
            }
            if (name == "Saffron")
            {

            }
            if (name == "")
            {

            }
            if (name == "Water")
            {

            }
        }
        if (tag == "boil")
        {
            if (name == "Almond")
            {
                Countdown("newFoodName");
            }
            if (name == "Saffron")
            {

            }
            if (name == "")
            {

            }
            if (name == "Water")
            {

            }
        }
        if (tag == "sliver")
        {
            if (name == "Almond")
            {
                Countdown("newFoodName");
            }
            if (name == "Saffron")
            {

            }
            if (name == "")
            {

            }
            if (name == "Water")
            {

            }
        }
        if (tag == "pound")
        {
            if (name == "Almond")
            {
                Countdown("newFoodName");
            }
            if (name == "Saffron")
            {

            }
            if (name == "")
            {

            }
            if (name == "Water")
            {

            }
        }
        if (tag == "press")
        {
            if (name == "Almond")
            {
                Countdown("newFoodName");
            }
            if (name == "Saffron")
            {

            }
            if (name == "")
            {

            }
            if (name == "Water")
            {

            }
        }
    }

    void Countdown(string newFood)
    {
        if (!isCooking)
        {
            StartCoroutine("CookTimeStartGo", newFood);
        }
    }

    IEnumerator CookTimeStartGo(string newFood)
    {
        float percentComplete = 0f;
        while (percentComplete != 1)
        {
            percentComplete += Time.deltaTime;

            yield return null;
        }

        Debug.Log("Ding");
        isCooking = false;
        //Instantiate(newFood);
        //Destroy(thisFood)

    }

    public void Update() {
    }
}