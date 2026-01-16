using UnityEngine;
using System.Collections;

public static class InputManager {

	// -- Setup for keyboard and Xbox One

	// -- Axis

	// -- Left Stick
	public static float MainHorizontal()
	{
		float result = 0.0f;
		result += Input.GetAxis("J_MainHorizontal");
		result += Input.GetAxis("K_MainHorizontal");
		return Mathf.Clamp(result, -1.0f, 1.0f);
	}

	public static float MainVertical()
	{
		float result = 0.0f;
		result += Input.GetAxis("J_MainVertical");
		result += Input.GetAxis("K_MainVertical");
		return Mathf.Clamp(result, -1.0f, 1.0f);
	}

	public static Vector3 MainJoystick()
	{
		return new Vector3(MainHorizontal(), 0, MainVertical());
	}

	// -- Right Stick
	public static float SubHorizontal()
	{
		float result = 0.0f;
		result += Input.GetAxis("J_SubHorizontal");
		result += Input.GetAxis("K_SubHorizontal");
		return Mathf.Clamp(result, -1.0f, 1.0f);
	}

	public static float SubVertical()
	{
		float result = 0.0f;
		result += Input.GetAxis("J_SubVertical");
		result += Input.GetAxis("K_SubVertical");
		return Mathf.Clamp(result, -1.0f, 1.0f);
	}

	public static Vector3 SubJoystick()
	{
		return new Vector3(SubHorizontal(), 0, SubVertical());
	}

	// -- Right Trigger
	public static float RTrigger()
	{
		float result = 0.0f;
		result += Input.GetAxis("R_Trigger");
		result += Input.GetAxis("KR_Trigger");
		return Mathf.Clamp(result, -1.0f, 1.0f);
	}

	// -- Left Trigger
	public static float LTrigger()
	{
		float result = 0.0f;
		result += Input.GetAxis("L_Trigger");
		result += Input.GetAxis("KL_Trigger");
		return Mathf.Clamp(result, -1.0f, 1.0f);
	}

	// -- Face Buttons 
	public static bool AButton(){ return Input.GetButtonDown("A_Button");}
	public static bool BButton(){ return Input.GetButtonDown("B_Button");}
	public static bool XButton(){ return Input.GetButtonDown("X_Button");}
	public static bool YButton(){ return Input.GetButtonDown("Y_Button");}

	// -- DPad Buttons 
	public static bool DPadUpButton(){ return Input.GetButtonDown("DPad_Up");}
	public static bool DPadDownButton(){ return Input.GetButtonDown("DPad_Down");}
	public static bool DPadLeftButton(){ return Input.GetButtonDown("DPad_Left");}
	public static bool DPadRightButton(){ return Input.GetButtonDown("DPad_Right");}

	// -- Bumpers
	public static bool LBButton(){ return Input.GetButton("LB_Button");}
	public static bool RBButton(){ return Input.GetButton("RB_Button");}

	// -- Extra
	public static bool StartButton(){ return Input.GetButton("Start_Button");}
}