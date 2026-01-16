using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class LivesManager : MonoBehaviour {

    private static int PLAYER_LIVES;
    private static int OFFSET = 60;

	// Use this for initialization
	void Start () {
        UpdateLives(3);
	}
	
	// Update is called once per frame
	void Update () {
	    
	}

    public void UpdateLives(int change)
    {
        PLAYER_LIVES += change;
        UpdateUI();
    }

    void UpdateUI()
    {
        DestroyObjectsWithTag("LifeIndicator");

        for (int i = 0; i < PLAYER_LIVES; i++)
        {
            GameObject instantiatedUI = Instantiate(Resources.Load("Prefabs/Life Indicator")) as GameObject;
            instantiatedUI.transform.SetParent(GameObject.Find("Score & Life Panel").transform);
            Image rect = instantiatedUI.GetComponent<Image>();
            rect.rectTransform.offsetMax = new Vector2(-19-(OFFSET*i), -10);
            rect.rectTransform.offsetMin = new Vector2(-57-(OFFSET*i), -50);
        }
    }

    void DestroyObjectsWithTag(string tagToDestroy)
    {
        GameObject[] go = GameObject.FindGameObjectsWithTag(tagToDestroy);

        foreach (GameObject g in go)
        {
            Destroy(g);
        }
    }
}