
using UnityEngine;

public class Gamemanager : MonoBehaviour
{
	
	GameObject[] pause;

	// Start is called before the first frame update
	void Start()
	{
		pause = GameObject.FindGameObjectsWithTag("pause");
		foreach (GameObject pauobj in pause)
		{
			pauobj.SetActive(false);
		}

	}

	// Update is called once per frame
	void Update()
	{

	}

		public void Pauseoption()
	{
		Time.timeScale = 0;
		foreach (GameObject pauobj in pause)
		{
			pauobj.SetActive(true);
		}
	}
	public void Playoption()
	{
		Time.timeScale = 1;
		foreach (GameObject pauobj in pause)
		{
			pauobj.SetActive(false);
		}
	}
}