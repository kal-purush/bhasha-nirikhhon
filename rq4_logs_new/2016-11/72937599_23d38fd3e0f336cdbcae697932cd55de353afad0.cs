using UnityEngine;
using System.Collections;

public class MenuBehaviourScript : MonoBehaviour {


	public GameObject start;
	public GameObject dificuldade;
	public GameObject visao;
	public GameObject menu;
	public GameObject easyer;
	public GameObject harder;


	private bool centro = false;
	private bool hard = false;
	private bool easy = false;
	private bool primeira = true;
	void Start () {
		Instantiate(start, menu.transform);
		visao.transform.position = new Vector3 (menu.transform.position.x, 0, -10);
	
	
	}
	void Update ()
	{
		if (primeira && Input.GetKeyDown ("Enter")) {
			primeira = false;
			start.transform.position = new Vector3 (0, 0, -15);
			Instantiate (dificuldade, menu.transform);
			centro = true;
		}
		if (Input.GetKeyDown ("Esc")) {
			primeira = true;
			start.transform.position = menu.transform.position;
			centro = false;
			hard = false;
			easy = false;
		}
		if (centro && Input.GetKeyDown ("D")) {
			visao.transform.position = new Vector3 (easyer.transform.position.x, 0, -10);
			centro = false;
			hard = true;
		}
		if (centro && Input.GetKeyDown ("A")) {
			visao.transform.position = new Vector3 (harder.transform.position.x, 0, -10);
			centro = false;
			hard = true;
		}
		if (hard && Input.GetKeyDown ("A")) {
			visao.transform.position = new Vector3 (easyer.transform.position.x, 0, -10);
			centro = false;
			easy = true;
		}
		if (hard && Input.GetKeyDown ("D")) {
			visao.transform.position = new Vector3 (dificuldade.transform.position.x, 0, -10);
			centro = true;
			hard = false;
		}
		if (easy && Input.GetKeyDown ("D")) {
			visao.transform.position = new Vector3 (harder.transform.position.x, 0, -10);
			easy = false;
			hard = true;
		}
		if (easy && Input.GetKeyDown ("A")) {
			visao.transform.position = new Vector3 (dificuldade.transform.position.x, 0, -10);
			centro = true;
			easy = false;
		}
		if (easy && Input.GetKeyDown ("Enter")) {
			//setar a vida chamando o script do player e mudando o valor da variavel
			Application.LoadLevel ("movementjj");
		
		}
		if (hard && Input.GetKeyDown ("Enter")) {
			//setar a vida chamando o script do player e mudando o valor da variavel
			Application.LoadLevel ("movementjj");
		}
  }
}