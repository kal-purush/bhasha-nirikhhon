using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Linq;
public class Inventory : MonoBehaviour {
	public IHoldable[] inventory;
	public GameObject playerObject;


	void Start () {
		inventory = new IHoldable[6];
		
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown ("Alpha 1")) {
			if (inventory [1]!= (null)) {
				inventory [0] = inventory [1];
			}
		
		}
		if (Input.GetKeyDown ("Alpha 2")) {
			if (inventory [2]!= (null)) {
				inventory [0] = inventory [2];
			}

		}
		if (Input.GetKeyDown ("Alpha 3")) {
			if (inventory [3]!= (null)) {
				inventory [0] = inventory [3];
			}

		}
		if (Input.GetKeyDown ("Alpha 4")) {
			if (inventory [4]!= (null)) {
				inventory [0] = inventory [4];
			}

		}
		if (Input.GetKeyDown ("Alpha 5")) {
			if (inventory [5]!= (null)) {
				inventory [0] = inventory [5];
			}

		}
		if (Input.GetMouseButton (0)) {
			if (inventory [0] is IWeapon) {
				(IWeapon)inventory [0].Fire ();
			}
		}
	}
	int addItem(IHoldable item){
		int counter = 0;
		if (inventory.Contains (item)) {
			return 0;
		} 
		else {
			for (int i = 1; i < inventory.Length; i++) {
				if (inventory [i] = null) {
					inventory [i] = item;
					break;
				} else {
					if(counter==inventory.Length-1){
						ReplaceInventory (item);
					}
					else{
						counter++;
					}
				}
			}
			return 1;
		}
		
	}
	int ReplaceInventory( IHoldable item ){
		IHoldable repalce = inventory [0];
		for (int i = 1; i < inventory.Length; i++) {
			if (inventory [i].Equals (repalce)) {
				inventory [i] = item;
				inventory [0] = item;
			}
			
		}
	
	}


}