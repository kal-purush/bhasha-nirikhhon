using UnityEngine;
using System.Collections;

public class Dialog : MonoBehaviour {
	Vector3 mouseOffset;

	void Start () {
	
	}
	
	void Update () {
	
	}

	public void CloseDialog() {
		Destroy(gameObject);
	}

	public void StartDrag() {
		transform.SetAsLastSibling();
		mouseOffset = transform.position - Input.mousePosition;
		UIManager.isDragging = true;
	}

	public void StopDrag() {
		UIManager.isDragging = false;
	}

	public void Drag() {
		transform.position = Input.mousePosition + mouseOffset;
		ClampPos();
	}

	public void PointerEnter() {
		UIManager.pointerEnter();
	}

	public void PointerExit() {
		UIManager.pointerExit();
	}

	void ClampPos() {
		if (transform.position.x < 0 + (GetWidth() / 2)) {
			transform.position = new Vector3(GetWidth() / 2, transform.position.y, 0);
		}

		if (transform.position.x > Screen.width - (GetWidth() / 2)) {
			transform.position = new Vector3(Screen.width - (GetWidth() / 2), transform.position.y, 0);
		}

		if (transform.position.y < 0 + (GetHeight() / 2)) {
			transform.position = new Vector3(transform.position.x, GetHeight() / 2, 0);
		}

		if (transform.position.y > Screen.height - (GetHeight() / 2)) {
			transform.position = new Vector3(transform.position.x, Screen.height - (GetHeight() / 2), 0);
		}
	}

	float GetWidth() {
		RectTransform r = GetComponent<RectTransform>();
		return r.rect.width;
	}

	float GetHeight() {
		RectTransform r = GetComponent<RectTransform>();
		return r.rect.height;
	}
}