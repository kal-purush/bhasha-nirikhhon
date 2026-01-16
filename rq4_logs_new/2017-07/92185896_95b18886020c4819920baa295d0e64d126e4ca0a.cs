using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Puzzle : MonoBehaviour {

    private QuestManager theQM;
    private DialogueManager theDM;
    private InventoryManager theIM;
    public int questNumber;


    

    // Safe variables
    [SerializeField]
    private InputField inputLeft;
    [SerializeField]
    private InputField inputMiddle;
    [SerializeField]
    private InputField inputRight;

    private int left;
    private int leftGuess;

    private int middle;
    private int middleGuess;

    private int right;
    private int rightGuess;

    // Use this for initialization
    void Start () {
        theQM = FindObjectOfType<QuestManager>();
        theDM = FindObjectOfType<DialogueManager>();
        theIM = FindObjectOfType<InventoryManager>();

        left = 5;
        middle = 5;
        right = 3;

    }
	
	// Update is called once per frame
	void Update () {
		
	}

    public void SetItemSolved(QuestObject qo)
    {
        qo.solved = true;
    }

    public void GetInput(string guess)
    {
        Debug.Log("You Entered " + guess);
        Debug.Log("Input Left " + inputLeft.text);
        Debug.Log("Input Middle " + inputMiddle.text);
        Debug.Log("Input Right " + inputRight.text);

        if (inputLeft.text == null || inputMiddle.text == null || inputRight.text == null)
        {
            theDM.ShowBox("You need 3 numbers combinations to open the safe.");
            Debug.Log("You need 3 numbers combinations to open the safe.");
        }

        if (inputLeft.text != null && inputMiddle.text != null && inputRight.text != null)
        {
            ComparedGuesses(int.Parse(inputLeft.text), int.Parse(inputMiddle.text), int.Parse(inputRight.text));
        }

    }

    void ComparedGuesses(int leftGuess, int middleGuess, int rightGuess)
    {
        if (leftGuess == left && middleGuess == middle && rightGuess == right)
        {
            inputLeft.GetComponent<UnityEngine.UI.Image>().color = Color.green;
            inputMiddle.GetComponent<UnityEngine.UI.Image>().color = Color.green;
            inputRight.GetComponent<UnityEngine.UI.Image>().color = Color.green;

            theIM.iBoxes[0].isSelected = false;
            theIM.iBoxes[0].GetComponent<UnityEngine.UI.Image>().color = new Color32(228, 207, 192, 255);
            theIM.iBoxes[1].isSelected = false;
            theIM.iBoxes[1].GetComponent<UnityEngine.UI.Image>().color = new Color32(228, 207, 192, 255);

            theIM.iBoxes[0].ShowItem("Journal");
            theIM.iBoxes[1].ShowItem("Flint");

            SetItemSolved(theQM.quests[questNumber]);
        }
        else
        {
            Debug.Log("Wrong combinations! Flahs Red!");
            inputLeft.GetComponent<UnityEngine.UI.Image>().color = Color.red;
            inputMiddle.GetComponent<UnityEngine.UI.Image>().color = Color.red;
            inputRight.GetComponent<UnityEngine.UI.Image>().color = Color.red;
        }
    }
}