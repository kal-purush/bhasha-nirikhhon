using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Portal : MonoBehaviour {
	public Camera observer;
	public Transform portalView;
	public Camera exitCamera;
	public UnityEngine.UI.RawImage portalImage;
//	public Transform viewport;
	public Transform exit;
	RenderTexture renderTexture;
	bool renderLogic = true;
//	public float rotatePortalViewSpeed = 180;
	Collider m_collider;

	private bool IsSeen() {
		Plane[] planes = GeometryUtility.CalculateFrustumPlanes(observer);
		return (GeometryUtility.TestPlanesAABB (planes, m_collider.bounds));
	}

	void Update() {
		renderLogic = IsSeen();
		if (renderLogic) {
			portalImage.gameObject.SetActive (true);
		} else {
			portalImage.gameObject.SetActive (false);
			return;
		}
		if (renderTexture != null) {
			if (renderTexture.width != Screen.width || renderTexture.height != Screen.height) {
				renderTexture.Release ();
				if (renderTexture.width != Screen.width) { renderTexture.width = Screen.width; }
				if (renderTexture.height != Screen.height) { renderTexture.height = Screen.height; }
				renderTexture.Create ();
			}
		}
		Vector3 delta = observer.transform.position - transform.position;
		float dist = delta.magnitude;
		exitCamera.transform.position = exit.position + delta;
		exitCamera.transform.rotation = observer.transform.rotation;
		exitCamera.nearClipPlane = dist;
	}

//	public float extraRatio = 6.63f;
	void Start() {
		RectTransform r = portalImage.GetComponent<RectTransform> ();
		float distance = observer.nearClipPlane;
		distance = Vector3.Distance (observer.transform.position, portalView.transform.position);
		float ratio = 1 + observer.nearClipPlane * distance;
//		RectTransform rt = portalView.GetComponent<RectTransform> ();
		const float extraRatio = 6.63f; // TODO figure out why this number? it has to do with UI width/height...
		float 
		w = Screen.width  * ratio * portalView.lossyScale.x * extraRatio, 
		h = Screen.height * ratio * portalView.lossyScale.y * extraRatio;
		r.sizeDelta = new Vector2 (w, h);
		m_collider = GetComponent<Collider> ();
		renderTexture = new RenderTexture(Screen.width, Screen.height, 24);
		renderTexture.Create();
		exitCamera.targetTexture = renderTexture;
		portalImage.texture = renderTexture;
	}

	void LateUpdate() {
		if (portalImage != null && renderLogic) {
			Quaternion r = portalView.rotation;
//			viewport.Rotate (0, 0, rotatePortalViewSpeed*Time.deltaTime);
			portalView.rotation = r;
			float epsilon = 1 / 1024.0f;
			float distance = observer.nearClipPlane;
			distance = Vector3.Distance (observer.transform.position, portalView.transform.position);
			if (distance <= observer.nearClipPlane) {
				distance += epsilon;
			}
			portalImage.transform.localScale = Vector3.one * distance;
//			new Vector3 (globalScale.x/transform.lossyScale.x, globalScale.y/transform.lossyScale.y, globalScale.z/transform.lossyScale.z);
			portalImage.transform.position = observer.transform.position + observer.transform.forward * (distance);
			portalImage.transform.rotation = observer.transform.rotation;
		}
	}

	public void OnTriggerEnter(Collider c) {
		Vector3 offset = c.transform.position - transform.position;
		c.transform.position = exit.position + offset;
	}
}