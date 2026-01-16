using UnityEngine;
using System.Collections;

public class MenuBehaviourScript : MonoBehaviour {
	Animator anim;
	float escolha;
	private bool dificil;
	private bool centro = true;
	private bool hard = false;
	private bool easy = false;
	void Start () {
	}
	void Update ()
	{
		escolha = Input.GetAxis ("Horizontal");
		//anim.SetBool ("Ligado", hard);
		if (centro && escolha > 0) {
			centro = false;
			hard = true;
		}
		if (centro && escolha < 0) {
			centro = false;
			easy = true;
		}
		if (hard && escolha < 0) {
			hard = false;
			easy = true;
		}
		if (easy && escolha > 0) {
			easy = false;
			hard = true;
		}
		if (easy) {
			if (Input.GetKey (KeyCode.KeypadEnter) || Input.GetKey ("enter")) {
				dificil = false;
				Application.LoadLevel ("Stage 1");
			}
		}
		if (hard) {
			if (Input.GetKey (KeyCode.KeypadEnter) || Input.GetKey ("enter")) {
				dificil = true;
				Application.LoadLevel ("Stage 1");
			}
		}
  	}
}