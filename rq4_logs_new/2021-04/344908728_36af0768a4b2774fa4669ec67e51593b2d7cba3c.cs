using Unity.Collections;
using UnityEngine;
using UnityEngine.UI;

public class Cronometer : MonoBehaviour
{
	
	public float initialSeconds;

	[ReadOnly]
	public float _seconds;

    private void Start()
    {
		_seconds = initialSeconds;
    }

    // Update is called once per frame
    void Update()
    {
		if (_seconds > 0f)
		{
			_seconds -= Time.deltaTime;

			int hours = Mathf.FloorToInt(_seconds / 3600F);
			int minutes = Mathf.FloorToInt((_seconds % 3600) / 60);
			int seconds = Mathf.FloorToInt(_seconds % 60);

			//format numer to 00:00:00
			gameObject.GetComponent<Text>().text = string.Format("{0:00}:{1:00}:{2:00}", hours, minutes, seconds);
		}
		else
		{
			_seconds = 0;
			gameObject.GetComponent<Text>().text = "00:00:00";
		}

	}

	public void initializeTimer()
	{
		_seconds = initialSeconds;

	}

	public void setTimer(float seconds)
	{
		_seconds = seconds;

	}
}