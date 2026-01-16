using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;

public class ButtonPanel : MonoBehaviour , IPointerEnterHandler, IPointerExitHandler
{

	[SerializeField]
	private GameObject buttonPanelObj;
	
	private ItemData data;
	private float timer;
	private bool launchTimer;
	
	private void Start() {
		ResetTimer();
		DeActivate();
	}
	
	private void Update() {
		if(launchTimer) {
			timer -= Time.deltaTime;
			if(timer <= 0)
				DeActivate();
		}
	}
	
	public void Activate(ItemData data) {
		this.data = data;
		buttonPanelObj.SetActive(true);
		buttonPanelObj.transform.position = data.gameObject.transform.position;
	}
	
	public void DeActivate() {
		ResetTimer();
		buttonPanelObj.SetActive(false);
	}
	
	public void OnPointerExit(PointerEventData eventData) {
		launchTimer = true;
	}
	
	public void OnPointerEnter(PointerEventData eventData) {
		ResetTimer();
	}
	
	private void ResetTimer() {
		launchTimer = false;
		timer = 2.0f;
	}
}