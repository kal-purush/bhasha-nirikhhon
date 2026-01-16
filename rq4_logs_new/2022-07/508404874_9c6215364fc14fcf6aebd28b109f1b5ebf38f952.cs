using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System.Collections.Specialized;

public class InventoryBar : MonoBehaviour
{
    [SerializeField] private List<Button> inventorySlots = new List<Button>(6);
    [SerializeField] private InventoryManager inventoryManager;

    private void Awake()
    {
        inventoryManager.InventorySlots.CollectionChanged += new NotifyCollectionChangedEventHandler(ManageItems);
    }

    public void Update()
    {
        if (Input.GetKey(KeyCode.Alpha1)) { ChooseItem(1); }
        if (Input.GetKey(KeyCode.Alpha2)) { ChooseItem(2); }
        if (Input.GetKey(KeyCode.Alpha3)) { ChooseItem(3); }
        if (Input.GetKey(KeyCode.Alpha4)) { ChooseItem(4); }
        if (Input.GetKey(KeyCode.Alpha5)) { ChooseItem(5); }
        if (Input.GetKey(KeyCode.Alpha6)) { ChooseItem(6); }

    }

    private void ChooseItem(int itemID)
    {

    }

    private void ManageItems(object sender, NotifyCollectionChangedEventArgs e)
    {
        int i = inventoryManager.InventorySlots.Count - 1;
        Button button = inventorySlots[i];
        if (e.Action == NotifyCollectionChangedAction.Add)
        {
            RawImage img = button.transform.GetChild(0).GetComponent<RawImage>();
            img.color = new Color(img.color.r, img.color.g, img.color.b, 255);
            img.texture = inventoryManager.InventorySlots[i].Image;
        }
    }

}