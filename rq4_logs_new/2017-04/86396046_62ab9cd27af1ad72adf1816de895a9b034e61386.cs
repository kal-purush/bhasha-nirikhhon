using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CombiManager : MonoBehaviour {
    public int Size = 5;

    private List<VSCombinationHandler.Button> CurrentCombination = new List<VSCombinationHandler.Button>();
    public enum Button { RED = 0, GREEN = 1, BLUE = 2, YELLOW = 3 };
    // Use this for initialization
    void Start () {
    }
	
    public void CreateCombination()
    {
        CurrentCombination.Clear();
        int color = 0;
        for (int i = 0; i < Size; i++)
        {
            color = Random.Range(0, 3);
            if (color == 0)
                CurrentCombination.Add(VSCombinationHandler.Button.RED);
            if (color == 1)
                CurrentCombination.Add(VSCombinationHandler.Button.GREEN);
            if (color == 2)
                CurrentCombination.Add(VSCombinationHandler.Button.BLUE);
            if (color == 3)
                CurrentCombination.Add(VSCombinationHandler.Button.YELLOW);
        }

        string test = "";

        foreach (CombinationHandler.Button x in CurrentCombination)
        {
            test = test + " " + x;
        }
        Debug.Log("CurrentCombination =" + test);
        Debug.Log("CurrentCombination SIZE =" + CurrentCombination.Count);
    }

    public List<VSCombinationHandler.Button> GetCombination()
    {
        return CurrentCombination;
    }
	// Update is called once per frame
	void Update () {
		
	}
}