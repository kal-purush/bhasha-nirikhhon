using UnityEngine;
using UnityEngine.UI;

public class PortraitBehaviour : MonoBehaviour {

	[SerializeField]
	private GameObject player;

	private Image boussole;
	private Text level;
	private PlayerXPHandler playerXP;
	
	private void Start() {
		boussole = GetComponentInChildren<Image>();
		
		playerXP = player.GetComponent<PlayerXPHandler>();
		level = GetComponentInChildren<Text>();
	}
	
	private void Update () {
		boussole.gameObject.transform.rotation = Quaternion.Euler(0,0,-player.transform.rotation.eulerAngles.y);
		level.text = playerXP.GetLevel().ToString();
	}
}