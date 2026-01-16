using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HealthController : MonoBehaviour
{
    private const float MAX_HEALTH = 100f;
    public float messyRate = 0.001f;
    public float cleaningRate = 0.01f;
    public Image SquareBar;

    private bool inRange;

    private float total_happiness = 100f;

    // Start is called before the first frame update
    void Start()
    {
        inRange = false;
        //SquareBar = GetComponent<Image>();
    }

    // Update is called once per frame
    void Update()
    {
        if (inRange)
        {
            total_happiness += MAX_HEALTH * cleaningRate;
        }
        if (inRange == false)
        {
            total_happiness -= MAX_HEALTH * messyRate;
        }
        
        if (total_happiness > MAX_HEALTH)
        {
            total_happiness = MAX_HEALTH;    
        }
        if (total_happiness < 0)
        {
            total_happiness = 0;
        }

        Debug.Log(total_happiness / MAX_HEALTH); 
        SquareBar.fillAmount = total_happiness / MAX_HEALTH;
    }

    private void OnTriggerStay2D(Collider2D collision)
    {
        //Debug.Log(collision.gameObject.tag);

        if (collision.gameObject.tag == "Player")
        {
            //Debug.Log("Grabbing Mop");
            inRange = true;
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            //Debug.Log("All cleaned up");
            inRange = false;
        }
    }
}