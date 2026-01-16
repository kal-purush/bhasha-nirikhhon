using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

public class Popup : MonoBehaviour
{
	[HideInInspector]
	public UnityEvent onClose;
	[HideInInspector]
	public UnityEvent onOpen;

	private void Awake()
	{
		if (onClose == null)
		{
			onClose = new UnityEvent();
		}

		if (onOpen == null)
		{
			onOpen = new UnityEvent();
		}
	}

	public void Close()
	{
		Time.timeScale = gameObject.activeSelf ? 0 : 1;
		onClose.Invoke();
	}

	public void Open()
	{
		Time.timeScale = gameObject.activeSelf ? 0 : 1;
		onOpen.Invoke();
	}
}