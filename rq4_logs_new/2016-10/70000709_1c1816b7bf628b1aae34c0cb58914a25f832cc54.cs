using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class Level : MonoBehaviour {

	public string levelText;
	public int unlocked; //0 = unlocked, 1 = locked

	public GameObject isUnlocked;
	public GameObject hasSecret;
	private Image[] image;

	void Start()
	{
		image = gameObject.transform.GetComponentsInChildren<Image>();
	}

	void Update()
	{
		if (unlocked == 0)
		{
			ChangeColor(image, Color.red);
		}
		else if (unlocked ==1)
		{
			ChangeColor(image, Color.green);
		}
	}

	void ChangeColor(Image[] array, Color color)
	{
		for (int i = 0; i < array.Length; i++)
		{
			array[i].color = color;
		}
	}
}