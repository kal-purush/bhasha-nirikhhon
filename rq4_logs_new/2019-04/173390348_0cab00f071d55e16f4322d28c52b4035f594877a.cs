using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerHealth : MonoBehaviour
{
    public GameObject heart1;
    public GameObject heart2;
    public GameObject heart3;

    [Header("Total Hit Points:")]
    public int hitPoints;

    // Start is called before the first frame update
    void Start()
    {
        hitPoints = 3;
    }

    // Update is called once per frame
    void Update()
    {
        // For TESTING /////////
        if (Input.GetKeyUp(KeyCode.H))
        {
            hitPoints--;
            if (hitPoints == 2)
            {
                heart3.SetActive(false);
            }
            if (hitPoints == 1)
            {
                heart2.SetActive(false);
            }
            if (hitPoints == 0)
            {
                heart1.SetActive(false);
                FindObjectOfType<CanvasControl>().ShowOverMenu();
            }
        }
        ////////////////////////
    }

    private void OnCollisionEnter(Collision col)
    {
        if (col.gameObject.CompareTag("Bear"))
        {
            hitPoints--;
            if (hitPoints == 2)
            {
                heart3.SetActive(false);
            }
            if (hitPoints == 1)
            {
                heart2.SetActive(false);
            }
            if (hitPoints == 0)
            {
                heart1.SetActive(false);
                FindObjectOfType<CanvasControl>().ShowOverMenu();
            }
        }
    }
}