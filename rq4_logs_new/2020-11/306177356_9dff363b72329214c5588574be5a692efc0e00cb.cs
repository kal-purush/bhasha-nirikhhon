using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class solutionBox : MonoBehaviour
{
    public GameObject correctClue;
    private bool correctlyAssigned;

    public solutionManager sm;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void check(GameObject g)
    {
        if (g == correctClue)
        {
            correctlyAssigned = true;
            sm.checkAllSolutions();
        }
    }

    public bool isCorrect()
    {
        return correctlyAssigned;
    }

    public void emptyBox()
    {
        correctlyAssigned = false;
    }

    public void lockPosition()
    {
        correctClue.GetComponent<drag>().LockPosition();

        correctClue.GetComponent<Image>().color = Color.green; //111, 226, 93
    }
}