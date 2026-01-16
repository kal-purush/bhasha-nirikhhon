using UnityEngine;
using System.Collections;

public class OptionItem : MonoBehaviour {

	public int index = 0;
	public string value;

	private OptionsController optionsController;
	private Animator anim;
	// Use this for initialization
	void Start () {
		optionsController = GameObject.FindGameObjectWithTag ("OptionsController").GetComponent<OptionsController>();
		anim = GetComponent<Animator> ();
	}

	void OnMouseDown() {
		optionsController.selectOption (index);
	}

	public void select(){
		anim.SetBool ("selected", true);
	}

	public void deselect(){
		anim.SetBool ("selected", false);
	}
}