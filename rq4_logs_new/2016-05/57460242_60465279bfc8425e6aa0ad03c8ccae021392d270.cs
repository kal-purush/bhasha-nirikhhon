using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Inventory : MonoBehaviour {

	private RectTransform inventoryRect;

	private float inventoryWidth, inventoryHight;

	public int slots;

	public int rows;

	public float slotPaddingLeft, slotPaddingTop;

	public float slotSize;

	public GameObject slotPrefab;
	///<summary>
	///List contains all slots in the inventory
	/// </summary>
	private List<GameObject> allSlots;
	///<summary>
	///keeps count how many empty slots are in inventory
	///</summary>
	private static int emptySlots;
	///<summary>
	///makes possible to call inventory.emptyslots++ at slot.cs what adds empty slot to emptyslots
	/// </summary>
	public static int EmptySlots{
		get{return emptySlots; }
		set{emptySlots = value; }
	}
	/// <summary>
	/// Use this for initialization
	/// </summary>
	void Start () {
		CreateLayout ();
	}
	///<summary>
	/// Update is called once per frame
	/// </summary>
	void Update () {
	
	}

	private void CreateLayout(){
		allSlots = new List<GameObject> ();

		emptySlots = slots;

		inventoryWidth = (slots / rows) * (slotSize + slotPaddingLeft) + slotPaddingLeft;

		inventoryHight = rows * (slotSize + slotPaddingTop) + slotPaddingTop;

		inventoryRect = GetComponent<RectTransform> ();

		inventoryRect.SetSizeWithCurrentAnchors (RectTransform.Axis.Horizontal, inventoryWidth);
		inventoryRect.SetSizeWithCurrentAnchors (RectTransform.Axis.Vertical, inventoryHight);
	
		int columns = slots / rows;

		for (int y = 0; y < rows; y++) {
			for (int x = 0; x < columns; x++) {
				GameObject newSlot = (GameObject)Instantiate (slotPrefab);

				RectTransform slotRect = newSlot.GetComponent<RectTransform> ();
				/// <summary>
				///renameing new slot
				/// </summary>
				newSlot.name = "Slot";
				///<summary>
				///Making the new slot to be child of the canvas
				/// </summary>
				newSlot.transform.SetParent (this.transform.parent);
				/// <summary>
				///setting slots position
				/// </summary>
				slotRect.localPosition = inventoryRect.localPosition + new Vector3 (slotPaddingLeft * (x + 1) + (slotSize * x), -slotPaddingTop * (y + 1) - (slotSize * y));
				///<summary>
				///Setting the size of slots
				/// </summary>
				slotRect.SetSizeWithCurrentAnchors (RectTransform.Axis.Horizontal, slotSize);
				slotRect.SetSizeWithCurrentAnchors (RectTransform.Axis.Vertical, slotSize);
				///<summary>
				///Adding newslots to allslots list
				/// </summary>
				allSlots.Add (newSlot);
			}
		}
	}

	public bool AddItem(Item item){
		if (item.maxSize == 1) {
			PlaceEmpty (item);
			return true;
		} else {
			foreach (GameObject slot in allSlots) {
				Slot tmp = slot.GetComponent<Slot> ();
				if (!tmp.IsEmpty) {
					if (tmp.CurrentItem.type == item.type && tmp.IsAvailable) {
						tmp.AddItem (item);
						return true;
					}
				}
			}
			if (emptySlots > 0){
				PlaceEmpty (item);
			}
		}
		return false;
	}
	///<summary>
	///finds empty slot and places the item there
	/// </summary>
	private bool PlaceEmpty(Item item){
		if (emptySlots > 0) {
			foreach (GameObject slot in allSlots) {
				Slot tmp = slot.GetComponent<Slot> ();
				if (tmp.IsEmpty) {
					tmp.AddItem (item);
					emptySlots--;
					return true;
				}
			}
		}
		return false;
	}
}