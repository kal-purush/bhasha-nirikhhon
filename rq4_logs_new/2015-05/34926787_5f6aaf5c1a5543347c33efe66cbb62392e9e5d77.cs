using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class cutscene : MonoBehaviour {

	public Animator cameraAnimator;
	public Text story;
	private bool launched = false;
	public Color alpha = new Color(1,1,1,1);

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		if(Input.GetButton("A")){
			//Application.LoadLevel (1);
		
			launched = true;
			cameraAnimator.SetTrigger("Go");

		}

		if (alpha.a > 0 && launched)
		{
			alpha.a -= Time.deltaTime;
			story.color = alpha;
		}
	
	}

	void StartGame (){
		Application.LoadLevel (1);
	}
}