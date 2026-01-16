using System.Collections;
using System.Collections.Generic;
using System.Data;
using UnityEngine;

public class Door : MonoBehaviour
{
	enum Condition
	{
		None,
		Proximity,
		AllPlayers,
		Key,
		Manual
	}

	[SerializeField] private Condition openCondition;
	[SerializeField] private Condition closeCondition;
	[SerializeField] private bool startClosed;
	[SerializeField] private float openCloseTime;
	[SerializeField] private Vector3 closePosition;
	[SerializeField] private Vector3 closeRotation;
	[SerializeField] private Vector3 openPosition;
	[SerializeField] private Vector3 openRotation;

	private SphereCollider trigger;
	private int playerCount = 0;
	private bool opened;

	private void Start()
    {
		if (openCloseTime <= 0.0f) { openCloseTime = 0.01f; }

		if (startClosed)
		{
			transform.position = closePosition;
			transform.eulerAngles = closeRotation;
			opened = false;
		}
		else
		{
			transform.position = openPosition;
			transform.eulerAngles = openRotation;
			opened = true;
		}

		if(openCondition == Condition.Proximity || openCondition == Condition.AllPlayers ||
			closeCondition == Condition.Proximity || closeCondition == Condition.AllPlayers)
		{
			trigger = gameObject.AddComponent<SphereCollider>();
			trigger.isTrigger = true;
			trigger.radius = 3.2f / transform.localScale.x;
			trigger.center = Vector3.zero;
		}
    }

    private void Update()
    {
        //TODO: Check condition
	}

	private IEnumerator Open()
	{
		opened = true;
		bool done = false;
		float timer = 0.0f;
		float time = 1.0f / openCloseTime;

		Vector3 initialPosition = transform.position;
		Quaternion initialRotation = transform.rotation;
		Quaternion finalRotation = Quaternion.Euler(openRotation);

		while (!done)
		{
			timer += Time.deltaTime * time;
			transform.position = Vector3.Lerp(initialPosition, openPosition, timer);
			transform.rotation = Quaternion.Slerp(initialRotation, finalRotation, timer);

			done = timer >= 1.0f;

			yield return null;
		}
	}

	private IEnumerator Close()
	{
		opened = false;
		bool done = false;
		float timer = 0.0f;
		float time = 1.0f / openCloseTime;

		Vector3 initialPosition = transform.position;
		Quaternion initialRotation = transform.rotation;
		Quaternion finalRotation = Quaternion.Euler(closeRotation);

		while (!done)
		{
			timer += Time.deltaTime * time;
			transform.position = Vector3.Lerp(initialPosition, closePosition, timer);
			transform.rotation = Quaternion.Slerp(initialRotation, finalRotation, timer);

			done = timer >= 1.0f;

			yield return null;
		}
	}

	private void OnTriggerEnter(Collider other)
	{
		if(other.tag == "Player")
		{
			++playerCount;

			if(openCondition == Condition.Proximity || (openCondition == Condition.AllPlayers && playerCount == GameManager.Instance.PlayerCount) && !opened)
			{
				StartCoroutine(Open());
			}

			if (closeCondition == Condition.Proximity || (closeCondition == Condition.AllPlayers && playerCount == GameManager.Instance.PlayerCount) && opened)
			{
				StartCoroutine(Close());
			}
		}
	}

	private void OnTriggerExit(Collider other)
	{
		if (other.tag == "Player")
		{
			--playerCount;
		}
	}
}