using Mapbox.Unity.Map;
using UnityEngine;

namespace DataskopAR.Entities {

	public class BirdViewCamera : MonoBehaviour {

#region Fields

		[SerializeField] private AbstractMap map;
		[SerializeField] private Camera birdViewCamera;
		[SerializeField] private Transform userCameraTransform;

#endregion

#region Methods

		private void FixedUpdate() {

			Vector3 nextPos = new(userCameraTransform.position.x, 20, userCameraTransform.position.z);
			Quaternion nextRotation = Quaternion.Euler(new Vector3(90, map.transform.rotation.eulerAngles.y, 0));

			birdViewCamera.transform.SetPositionAndRotation(nextPos, nextRotation);

		}

#endregion

	}

}