using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class starRatings : MonoBehaviour
{
    [SerializeField] private int activeStars = 2;
    [SerializeField] private OutcomeManager outcomes;
    [SerializeField]private GameObject[] star = new GameObject[5];

    // Start is called before the first frame update
    void Start()
    {
        int i = activeStars;
        foreach (GameObject ster in star)
        {
            Debug.Log("checking star, i is " + i);
            if (i > 0)
            {
                ster.SetActive(true);
                i--;
            }
            else ster.SetActive(false);
        }
    }

    public void changeActiveStars(int change)
    {
        activeStars += change;
        int i = activeStars;
        foreach (GameObject ster in star)
        {
            if (i > 0)
            {
                ster.SetActive(true);
                i--;
            }
            else ster.SetActive(false);
        }

        if (activeStars <= 0)
        {
            outcomes.callLose();
        }
    }
}