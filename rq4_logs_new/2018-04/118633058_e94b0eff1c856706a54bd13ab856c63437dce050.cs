using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HUD : MonoBehaviour {

	public Inventory Inventory;

	// Use this for initialization
	void Start () {
		Inventory.ItemAdded += InventoryScript_ItemAdded;
		Inventory.ItemRemoved += Inventory_ItemRemoved;

	}
		
	private void InventoryScript_ItemAdded(object sender, InventoryEventArgs e){
		Transform inventoryPanel = transform.Find ("InventoryPanel");
		int counter = -1;
		foreach (Transform slot in inventoryPanel) {
			counter++;
			Transform imageTransform = slot.GetChild (0).GetChild (0); //sets imageTransform into image of slot
			Transform textTransform = slot.GetChild(0).GetChild(1);
			Image image = imageTransform.GetComponent<Image> ();
			Text textCount = textTransform.GetComponent<Text> ();
			ItemDrag itemDrag = imageTransform.GetComponent<ItemDrag> ();

			if (counter == e.Item.Slot.Id) {
				image.enabled = true;
				image.sprite = e.Item.Image;

				int itemCount = e.Item.Slot.Count;
				Debug.Log ("ITEM COUNT" + itemCount);
				if (itemCount >= 1)
					textCount.text = itemCount.ToString();
				else
					textCount.text = "";
				
//				itemDrag.Item = e.Item;
				itemDrag.Item = (ItemBaseClass)e.Item;

				break;

			}

//			//finding empty slot
//			if(image.sprite == null){
//				image.enabled = true;
//				image.sprite = e.Item.Image;
//
//				//TODO: Store a reference to item
//				break;
//			}

		}
	}

	void Inventory_ItemRemoved (object sender, InventoryEventArgs e)
	{
		Transform inventoryPanel = transform.Find ("InventoryPanel");
		int counter = -1;
		foreach (Transform slot in inventoryPanel) {
			counter++;

			Transform imageTransform = slot.GetChild (0).GetChild (0); //sets imageTransform into image of slot
			Transform textTransform = slot.GetChild (0).GetChild (1);
			Image image = imageTransform.GetComponent<Image> ();
			Text textCount = textTransform.GetComponent<Text> ();
			ItemDrag itemDrag = imageTransform.GetComponent<ItemDrag> ();

			if (itemDrag.Item == null)
				continue;
			
			if (e.Item.Slot.Id == counter) {
				int itemCount = e.Item.Slot.Count;
//				itemDrag.Item = e.Item.Slot.FirstItem;
				itemDrag.Item = (ItemBaseClass)e.Item.Slot.FirstItem;

				if (itemCount < 2) {
					textCount.text = "";
				} else {
					textCount.text = itemCount.ToString();
				}

				if (itemCount == 0) {
					image.enabled = false;
					image.sprite = null;
				}
				break;
			}
		}
	}
}