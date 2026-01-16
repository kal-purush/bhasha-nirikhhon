using UnityEngine;
using System.Collections;

public class PlayerSwitch : MonoBehaviour
{
	public Transform playerOne;

	public Transform playerTwo;

	void Update()
	{
		if (Input.GetKeyDown(KeyCode.R) && playerOne.GetComponentInChildren<Movement>().isActivePlayer)
		{
			playerOne.GetComponentInChildren<Movement>().setActivePlayer(false);
			playerTwo.GetComponentInChildren<Movement>().setActivePlayer(true);
		}
		else if (Input.GetKeyDown(KeyCode.R) && playerTwo.GetComponentInChildren<Movement>().isActivePlayer)
		{
			playerTwo.GetComponentInChildren<Movement>().setActivePlayer(false);
			playerOne.GetComponentInChildren<Movement>().setActivePlayer(true);
		}
	}
}