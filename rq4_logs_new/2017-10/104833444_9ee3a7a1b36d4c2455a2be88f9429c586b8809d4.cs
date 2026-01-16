using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MoveSelector : MonoBehaviour {

    public Text topText;
    public Text selectedText;
    public Text bottomText;

    public List<string> moves;

    public int currentSelection = 0;

    private float oldValue;
    private float threshold = .00f;

	// Use this for initialization
	void Start () {
        updateTextBoxes();
	}
	
	// Update is called once per frame
	void Update () {
        if (Mathf.Abs(oldValue) <= threshold && Mathf.Abs(Input.GetAxis("Vertical")) > threshold) {
            if (Input.GetAxis("Vertical") > 0) {
                currentSelection--;
            } else {
                currentSelection++;
            }
            if (currentSelection < 0) {
                currentSelection += moves.Count;
            }
            if (currentSelection > moves.Count) {
                currentSelection -= moves.Count;
            }
            updateTextBoxes();
        }


        oldValue = Input.GetAxis("Vertical");

    }

    public void updateTextBoxes() {
        topText.text = getCircular<string>(moves, currentSelection - 1);
        selectedText.text = getCircular<string>(moves, currentSelection);
        bottomText.text = getCircular<string>(moves, currentSelection + 1);
    }

    public T getCircular<T>(List<T> list, int index) {
        int len = list.Count;
        if (index >= len) {
            return getCircular<T>(list, index - len);
        }
        if(index < 0) {
            return getCircular<T>(list, index + len);
        }
        return list[index];
    }
}