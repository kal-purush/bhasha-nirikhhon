using UnityEngine;
using System.Collections;
using UnityEngine.UI;

[RequireComponent(typeof(Text))]
public class escape_index : MonoBehaviour {

	public GameObject pantaegi; 
	public Text main_Text;
	public void escape(){
		StartCoroutine("print_text");
	}

	IEnumerator print_text(){
		string[] str={"다행히 저 아이에게 \n뛸 수 있을 정도의 힘은\n 남아있었던 것 같다."
		};
		for(int j=0;j<str.Length;j++){
			for (int i=0; i<=str[j].Length; i++) {
				main_Text.text = str[j].Substring(0,i);
				yield return new WaitForSeconds(0.1f);
			}
			yield return new WaitForSeconds(1f);
		}
		pantaegi.SetActive (false);
		main_Text.text = "";
		GameObject.Find ("Text_text").GetComponent<text_manager_woodman> ().change_sentence (4);
	}
}