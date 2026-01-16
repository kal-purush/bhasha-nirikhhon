using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI; // Import UnityEngine.UI to use Text compone
using TMPro; // Import TMPro to use TextMeshPro components

public class ItemUI : MonoBehaviour
{
    [SerializeField]  public InventoryObject Inventory;
    [SerializeField]  public GameObject textOne;
    [SerializeField]  public GameObject textTwo;
    [SerializeField]  public GameObject textThree;
    [SerializeField]  public GameObject textFour;
    [SerializeField]  public GameObject textFive;

    Dictionary<InventoryObject.WeaponSlot, GameObject> itemsDisplayed = new Dictionary<InventoryObject.WeaponSlot, GameObject>();

    // Update is called once per frame
    void Update()
    {

        textOne.GetComponent<Text>().text = Inventory.Container[0].amount.ToString();
        textTwo.GetComponent<Text>().text = Inventory.Container[1].amount.ToString();
        textThree.GetComponent<Text>().text = Inventory.Container[2].amount.ToString();
        textFour.GetComponent<Text>().text = Inventory.Container[3].amount.ToString();
        textFive.GetComponent<Text>().text = Inventory.Container[4].amount.ToString();

    }

}