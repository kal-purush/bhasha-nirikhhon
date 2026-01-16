using UnityEngine;
using System.Collections;

public class manage_inven_global : MonoBehaviour {
	
	
	public int item_num;
	
	public bool[] item_list_used = new bool[7];
	void Start() {
		item_list_used[0] = false;
		item_list_used[1] = false;
		item_list_used[2] = false;
		item_list_used[3] = false;
		item_list_used[4] = false;
		item_list_used[5] = false;
		item_list_used[6] = false;
		item_num = 0;
	}
	
	public void push_item(GameObject input_item) {
		input_item.SetActive (true);
		item_num++;
		
		if (item_list_used[0]== false) {
			input_item.transform.position = gameObject.transform.position + new Vector3 (-13.2f, 4.96f, -0.21f) + 0f * new Vector3 (1f, 0f, 0f);
			item_list_used[0]= true;
			input_item.GetComponent<inven_num>().inven_list_num=0;
		} else if (item_list_used[1] == false) {
			input_item.transform.position = gameObject.transform.position +new Vector3(-13.2f, 4.96f, -0.21f) + 1f * new Vector3 (1f, 0f, 0f);
			item_list_used[1]= true;
			input_item.GetComponent<inven_num>().inven_list_num=1;
		} else if (item_list_used[2] == false) {
			input_item.transform.position = gameObject.transform.position +new Vector3(-13.2f, 4.96f, -0.21f) + 2f * new Vector3 (1f, 0f, 0f);
			item_list_used[2] = true;
			input_item.GetComponent<inven_num>().inven_list_num=2;
		}else if (item_list_used[3] == false) {
			input_item.transform.position = gameObject.transform.position +new Vector3(-13.2f, 4.96f, -0.21f)+ 3f * new Vector3 (1f, 0f, 0f);
			item_list_used[3]= true;
			input_item.GetComponent<inven_num>().inven_list_num=3;
		}else if (item_list_used[4]== false) {
			input_item.transform.position = gameObject.transform.position +new Vector3 (-13.2f, 4.96f, -0.21f) + 4f * new Vector3 (1f, 0f, 0f);
			item_list_used[4] = true;
			input_item.GetComponent<inven_num>().inven_list_num=4;
		}else if (item_list_used[5]== false) {
			input_item.transform.position = gameObject.transform.position +new Vector3 (-13.2f, 4.96f, -0.21f)+ 5f * new Vector3 (1f, 0f, 0f);
			item_list_used[5] = true;
			input_item.GetComponent<inven_num>().inven_list_num=5;
		}else if (item_list_used[6]== false) {
			input_item.transform.position = gameObject.transform.position +new Vector3 (-13.2f, 4.96f, -0.21f)+ 6f * new Vector3 (1f, 0f, 0f);
			item_list_used[6] = true;
			input_item.GetComponent<inven_num>().inven_list_num=6;
		}
	}
	
	public void pop_item(GameObject input_item){
		item_num--;
		
		
		item_list_used[input_item.GetComponent<inven_num>().inven_list_num] =false;
		
		input_item.SetActive (false);
		
	}
	
	
	
	
}