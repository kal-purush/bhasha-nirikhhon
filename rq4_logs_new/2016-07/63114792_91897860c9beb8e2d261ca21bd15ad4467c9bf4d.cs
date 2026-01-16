using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class RadialOnClick : MonoBehaviour
{
    public int buttonIndex;
    public List<GameObject> Blocks;

    // Use this for initialization
    public void onClick()
    {
        buttonIndex = 0;
        AttachBlockToController(buttonIndex);
    }

    // Update is called once per frame
    void Update() {
       
	}

    void AttachBlockToController(int buttonIndex)
    {
        var selectedBlock = Blocks[buttonIndex];

        selectedBlock.transform.SetParent(GameObject.Find("Controller (left)").transform);
    }
}