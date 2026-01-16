using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.EventSystems;
using System.Collections.Generic;

[System.Serializable]
public class Item {
    public string name;
    public Sprite icon;
    public string author;
}

public class ScriptCreateScrollList : MonoBehaviour {

    public GameObject sampleButton;
    public List<Item> itemList;

    public Transform contentPanel;

    void Start()
    {
        PopulateList();
    }
	
    void PopulateList()
    {
        foreach (Item item in itemList)
        {
            GameObject newButton = (GameObject)Instantiate(sampleButton);
            SampleButton button = newButton.GetComponent<SampleButton>();
            
            button.levelName.text = item.name;
            button.authorLabel.text = item.author;
            button.icon.sprite = item.icon;
            newButton.transform.SetParent(contentPanel);
            
        }
    }

}