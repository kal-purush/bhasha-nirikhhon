using UnityEngine;
using System.Collections;


public class PlayerMovement : MonoBehaviour {
	[System.Serializable]
	public class MouseLook
	{
		[Range(1.0f, 10.0f)]
		public float XSensitivity = 2.0f;
		[Range(1.0f, 10.0f)]
		public float YSensitivity = 2.0f;
		public bool clampVerticalRotation = true;
		public float MinimumX = -90.0F;
		public float MaximumX = 90.0F;
		public bool smooth;
		[Range(1.0f, 10.0f)]
		public float smoothTime = 5.0f;
		public bool lockCursor = true;
		Texture2D crossHair;

		Quaternion m_CharacterTargetRot;
		Quaternion m_CameraTargetRot;
		bool m_cursorIsLocked = true;

		public void LookRotation(Transform character, Transform camera)
		{
			float yRot = Input.GetAxis("Mouse X") * XSensitivity;
			float xRot = Input.GetAxis("Mouse Y") * YSensitivity;
			crossHair = Resources.Load("Crosshair") as Texture2D;

			m_CharacterTargetRot = character.localRotation;
			m_CameraTargetRot = camera.localRotation;

			m_CharacterTargetRot *= Quaternion.Euler(0f, yRot, 0f);
			m_CameraTargetRot *= Quaternion.Euler(-xRot, 0f, 0f);

			if (clampVerticalRotation)
				m_CameraTargetRot = ClampRotationAroundXAxis(m_CameraTargetRot);

			if (smooth)
			{
				character.localRotation = Quaternion.Slerp(character.localRotation, m_CharacterTargetRot, smoothTime * Time.deltaTime);
				camera.localRotation = Quaternion.Slerp(camera.localRotation, m_CameraTargetRot, smoothTime * Time.deltaTime);
			}
			else
			{
				character.localRotation = m_CharacterTargetRot;
				camera.localRotation = m_CameraTargetRot;
			}

			UpdateCursorLock();
		}

		void UpdateCursorLock()
		{
			//if the user set "lockCursor" we check & properly lock the cursos
			if (lockCursor)
				InternalLockUpdate();
		}

		void InternalLockUpdate()
		{
			if (Input.GetKeyUp(KeyCode.Escape))
			{
				m_cursorIsLocked = false;
			}
			else if (Input.GetMouseButtonUp(0))
			{
				m_cursorIsLocked = true;
			}

			if (m_cursorIsLocked)
			{
				Cursor.lockState = CursorLockMode.Confined;
				Cursor.SetCursor(crossHair, Vector2.zero, CursorMode.Auto);
			}
			else if (!m_cursorIsLocked)
			{
				Cursor.lockState = CursorLockMode.None;
				Cursor.SetCursor(null, Vector2.zero, CursorMode.Auto);
			}
		}

		Quaternion ClampRotationAroundXAxis(Quaternion q)
		{
			q.x /= q.w;
			q.y /= q.w;
			q.z /= q.w;
			q.w = 1.0f;

			float angleX = 2.0f * Mathf.Rad2Deg * Mathf.Atan(q.x);

			angleX = Mathf.Clamp(angleX, MinimumX, MaximumX);

			q.x = Mathf.Tan(0.5f * Mathf.Deg2Rad * angleX);

			return q;
		}

	}
	public Vector2[] wayPoints2D;
	public Camera mainCamera;
	[Range(1,10)]
	public int playerSpeed;
	Vector3[] wayPoints3D;
	Rigidbody rigidBody;
	int wayPointNumber;
	public MouseLook mouseLook;


	void Start()
	{
		//Cursor.SetCursor(null, Vector2.zero, CursorMode.ForceSoftware);
		wayPointNumber = 0;
		rigidBody = GetComponent<Rigidbody>();
		wayPoints3D = new Vector3[wayPoints2D.Length];
		for (int i = 0; i < wayPoints3D.Length; i++)
		{
			wayPoints3D[i] = ConvertWayPointTo3D(wayPoints2D[i]);
		}
		mouseLook = new MouseLook();
	}

	Vector3 ConvertWayPointTo3D(Vector2 wayPoint)
	{
		return new Vector3(wayPoint.x, 1.0f, wayPoint.y);
	}

	void Update()
	{
		mouseLook.LookRotation(transform,mainCamera.transform);
	}

	void FixedUpdate()
	{
		if (Vector3.Distance(transform.position, wayPoints3D[wayPointNumber]) < 0.5f)
		{
			if (wayPointNumber < (wayPoints3D.Length - 1))
			{
				wayPointNumber++;
			}
			else
			{
				wayPointNumber = 0;
			}       
		}
		else
		{
			rigidBody.MovePosition(transform.position + (wayPoints3D[wayPointNumber] - transform.position).normalized * playerSpeed * Time.deltaTime);
		}

	}
}