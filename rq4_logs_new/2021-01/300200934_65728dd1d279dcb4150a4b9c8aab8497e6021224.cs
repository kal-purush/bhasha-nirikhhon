using System;
using UnityEngine;
using UnityEngine.UI;

public class Graph : MonoBehaviour
{
    private float minX, minY, height, width;

    public GameObject pixel;
    public GameObject infectedPixel;
    public GameObject recoveredPixel;

    private InputField personSum, duration;

    private int maxPerson, maxDuration;

    private void Start()
    {
        minX = -22.579f;
        minY = 12.72f;
        height = 8;
        width = 40;

        gameObject.tag = "Player";

        personSum = GameObject.Find("PersonInput").GetComponent<InputField>();
        duration = GameObject.Find("DurationInput").GetComponent<InputField>();




    }

    private void Update()
    {
        maxPerson = System.Convert.ToInt32(personSum.text);
        maxDuration = System.Convert.ToInt32(duration.text);
    }

    public void buildGraph(int currentSecond)
    {
        float currentPercentageX = 100 / maxDuration * currentSecond;
        float currentPercentagePerson = 100 / maxPerson * GameHandler.personCounter;
        float currentPercentagePersonInfected = 100 / maxPerson * GameHandler.infectedPersonCounter;
        float currentPercentagePersonRecovered = 100 / maxPerson * GameHandler.recoveredPersonCounter;





        //float currentPostionX = width * (currentPercentageX / 100) + minX;
        float currentPostionX = currentSecond * 0.5f + minX;

        int numberPerson = Convert.ToInt32(height * (currentPercentagePerson / 100));
        int numberInfectedPerson = Convert.ToInt32(height * (currentPercentagePersonInfected / 100));
        int numberRecoveredPerson = Convert.ToInt32(height * (currentPercentagePersonRecovered / 100));

        float infectedPersonY = minY;
        float personY = infectedPersonY + (numberInfectedPerson  * 0.5f);      
        float recoverdPersonY = personY + (personY * 0.5f);


        createGameObject(pixel, numberPerson, currentPostionX, personY);
        createGameObject(infectedPixel, numberInfectedPerson, currentPostionX, infectedPersonY);
        createGameObject(recoveredPixel, numberRecoveredPerson, currentPostionX, recoverdPersonY);
    }

    public void createGameObject(GameObject theGameObject, int number, float currentPostionX, float currentPostionY)
    {
        for (int i = 0; i < number; i++)
        {
            float position = currentPostionY + (i * 0.5f);
            Instantiate(theGameObject, new Vector3(currentPostionX, position, 0), Quaternion.identity);
        }

        
    }
}