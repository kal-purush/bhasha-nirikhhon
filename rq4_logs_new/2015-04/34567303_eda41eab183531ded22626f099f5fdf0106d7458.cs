using UnityEngine;
using System.Collections;

public class Menu : MonoBehaviour {

	Animator animator;
	CanvasGroup canvasGroup;

	void Awake(){
		animator = GetComponent<Animator> ();
		canvasGroup = GetComponent<CanvasGroup> ();

		// centering our menu
		var rect = GetComponent<RectTransform>();
		rect.offsetMax = rect.offsetMin = new Vector2 (0, 0);

	}

	void Update(){
		//when the animation state is not open disable the Canvas else enable it
		if(!animator.GetCurrentAnimatorStateInfo(0).IsName("Open")){
			canvasGroup.blocksRaycasts = canvasGroup.interactable = false;
		}
		else{
			canvasGroup.blocksRaycasts = canvasGroup.interactable = true;
		}


	}

	public void setIsOpen(bool value){
		animator.SetBool("IsOpen",value);
	}
	public bool getIsOpen(){

		return animator.GetBool("IsOpen");
	}

}