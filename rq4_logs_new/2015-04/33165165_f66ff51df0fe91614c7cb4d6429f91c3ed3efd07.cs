using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using System.IO;

public class LevelSelectSubmenu : MonoBehaviour
{
	public GameObject levelDisplayPrefab;
	public PlayGameManager PGM;
	
	private const float levelDisplayHeight = 30f;
	
	
	void Start ()
	{	
		DirectoryInfo levelDir = new DirectoryInfo("Assets/Resources/Levels");
		FileInfo[] levelInfo = levelDir.GetFiles("*.*");
		
		float height = levelInfo.Length > 5 ? levelInfo.Length * levelDisplayHeight : 160f;
		
		transform.GetComponentInParent<RectTransform>().SetSizeWithCurrentAnchors(RectTransform.Axis.Vertical, height);
		
		int numLevel = 0;
		
		foreach(FileInfo level in levelInfo)
		{
			if(level.Name.EndsWith(".xml"))
			{
				GameObject newLevelDisplay = Instantiate(levelDisplayPrefab, new Vector3(0f, 0f, -2f), Quaternion.identity) as GameObject;
				
				newLevelDisplay.transform.SetParent(transform, true);
				newLevelDisplay.SetActive(true);
				
				newLevelDisplay.GetComponentInChildren<Canvas>().GetComponentInChildren<InputField>().text
					= level.Name.Remove(level.Name.Length - 4);
				
				AddSetCurrentLevelListener(newLevelDisplay.GetComponentInChildren<Button>(), level.Name.Remove(level.Name.Length - 4));
	
				newLevelDisplay.transform.position = new Vector3(0, levelDisplayHeight * numLevel, -2f);
				
				++numLevel;
			}
		}
	}
	
	// For each loops don't work with assigning listeners properly so we need this
	private void AddSetCurrentLevelListener(Button b, string name)
	{
		b.onClick.AddListener(() => PGM.SelectLevel(name));
	}
}