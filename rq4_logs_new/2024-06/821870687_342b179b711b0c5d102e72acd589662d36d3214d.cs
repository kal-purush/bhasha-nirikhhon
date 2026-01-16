using System;
using System.Collections;
using UnityEngine;
using UnityEngine.UI;

public class Counter : MonoBehaviour
{
	[SerializeField] private float _counter = 0f;
	[SerializeField] private float _additionConst = 1f;
	[SerializeField] private Text _text;

	private bool _IsCount = false;
	private Coroutine _coroutine;

	public void StartCount()
	{
		if (_IsCount = !_IsCount)
		{
			_coroutine = StartCoroutine(CountWithDelay());
		}
		else 
		{ 
			StopCoroutine(_coroutine);
		}
	}

	private IEnumerator CountWithDelay()
	{
		var waitTime = new WaitForSecondsRealtime(_additionConst);

		while (true) 
		{
			_text.text = Convert.ToString(++_counter);
			yield return waitTime;
		}
	}
}