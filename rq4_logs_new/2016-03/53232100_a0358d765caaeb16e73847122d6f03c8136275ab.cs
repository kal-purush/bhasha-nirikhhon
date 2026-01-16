using UnityEngine;
using System.Collections;

public class Joystick : MonoBehaviour 
{
	public Camera GUICamera;
	public bool mouseInput;

	private bool isActive;
	private Transform circleBig, circleSmall;
	private Vector2 startPos;
	private PlayerController player;
	private float cos = 0, sin = 0;

	void Awake () 
	{
		#if !UNITY_EDITOR
		mouseInput = false;
		#endif
		circleBig = transform.GetChild(0).transform;
		circleSmall = circleBig.GetChild(0).transform;
		circleBig.gameObject.SetActive(false);
		player = GameObject.FindObjectOfType<PlayerController>();
	}
		
	void Update()
	{
		circleBig.gameObject.SetActive(isActive);

		if (mouseInput)
		{
			if (Input.GetMouseButtonDown(0) && Input.mousePosition.x < Screen.width / 2.5f && Input.mousePosition.y < Screen.height / 1.5f)
				SetPos((Vector2)Input.mousePosition);
			else if (Input.GetMouseButton(0) && isActive)
					UpdateAxis(startPos - (Vector2)Input.mousePosition);
			else if (Input.GetMouseButtonUp(0))
				Unset();
			return;
		}

		if (Input.touchCount > 0 && !isActive)
		{
			var touch = Input.GetTouch(0);
			if (touch.position.x < Screen.width / 2f && touch.position.y < Screen.height / 2f && touch.phase == TouchPhase.Began)
			{
				isActive = true;
				startPos = touch.position;
				this.transform.position =  new Vector3(GUICamera.ScreenToWorldPoint(touch.position).x, GUICamera.ScreenToWorldPoint(touch.position).y, -1);
			}
		}
		else if (Input.touchCount > 0 && isActive)
			UpdateAxis(startPos - Input.GetTouch(0).position);
		else
			Unset();
	}

	void UpdateAxis(Vector2 deltaPosition)
	{
		float x = deltaPosition.x;
		float y = deltaPosition.y;
		float g = Mathf.Sqrt(x * x + y * y);
		if (x != 0 && y != 0)
		{
			cos = -x / g;
			sin = -y / g;
			circleSmall.transform.localPosition = new Vector3(cos, sin, -1f) * 0.1f;
		}
	}

	void SetPos(Vector2 pos)
	{
		isActive = true;
		startPos =  pos;
		this.transform.position =  new Vector3(GUICamera.ScreenToWorldPoint(startPos).x, GUICamera.ScreenToWorldPoint(startPos).y, -1);
	}

	void Unset()
	{
		isActive = false;
		cos = 0;
		sin = 0;
	}

	public Vector2 GetAxis()
	{
		return new Vector2(cos, sin);
	}
}