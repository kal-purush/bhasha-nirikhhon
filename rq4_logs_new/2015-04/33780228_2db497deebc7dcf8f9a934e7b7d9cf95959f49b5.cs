using UnityEngine;
using System.Collections;

[AddComponentMenu("Chromatix/Player/PlayerInput")]
public class PlayerInput : MonoBehaviour
{
	public string horizontal = "Horizontal";
	public string vertical = "Vertical";

	public string fire = "Fire1";
	public string jump = "Jump";
	
	public bool IsFirePressed()
	{
		return Input.GetButton(fire);
	}
	
	public float Horizontal()
	{
		return Input.GetAxis(horizontal);
	}
	
	public float Vertical()
	{
		return Input.GetAxis(vertical);
	}
	
	public bool IsJumpPressed()
	{
		return Input.GetButton(jump);
	}

	public bool IsJumpDown()
	{
		return Input.GetButtonDown(jump);
	}
}