using UnityEngine;
using GameEnums;

public class Button : MonoBehaviour
{
	[SerializeField]
	ButtonSpriteControl buttonSpriteControl;

	[SerializeField]
	ButtonState buttonState;

	private void OnTriggerEnter(Collider other)
	{
		if (other.tag == "Player")
		{
			SwitchState();
			buttonSpriteControl.UpdateSprite();
		}
	}

	public void SwitchState()
	{
		if (buttonState.GetState() == LaserColor.blue)
		{
			buttonState.SetState(LaserColor.yellow);
		}
		else if (buttonState.GetState() == LaserColor.yellow)
		{
			buttonState.SetState(LaserColor.blue);
		}
	}
}