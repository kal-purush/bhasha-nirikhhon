using UnityEngine;

namespace Player
{
	[RequireComponent(typeof(Camera))]
	public class PlayerCamera : MonoBehaviour
	{
		[Header("Vertical Bounds"), SerializeField] private float _minAngle;
		[SerializeField] private float _maxAngle;
		
		[Header("Sensitivity"), SerializeField] private float _horizontal;
		[SerializeField] private float _vertical;

		[SerializeField] private Vector3 _eulerAngle;

		private void Update()
		{
			// Get input from the player.
			var yaw = Input.GetAxis("Mouse X");
			var pitch = -Input.GetAxis("Mouse Y");

			// Change the current angle based on input.
			_eulerAngle += new Vector3(pitch * _vertical, yaw * _horizontal);

			// Clamp the pitch of the camera.
			var clampedPitch = Mathf.Clamp(_eulerAngle.x, _minAngle, _maxAngle);
			_eulerAngle = new Vector3(clampedPitch, _eulerAngle.y % 360);

			// Update the current rotation.
			transform.eulerAngles = _eulerAngle;
		}
	}
}