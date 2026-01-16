using UnityEngine;
using System.Collections;

public class buttonShopShip : MonoBehaviour {

    public int value;
    public int index;
    public bool dejaAcheter;
    public GameObject textValue;
    public GameObject imageValue;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}

    public void click()
    {
        if(dejaAcheter)
        {
            GameManager.instance.changeShip(index);
        }
        else
        {
            if(GameManager.instance.Gold >= value)
            {
                dejaAcheter = true;

                GameManager.instance.Gold = GameManager.instance.Gold - value;
                GameManager.instance.changeShip(index);

                textValue.SetActive(false);
                imageValue.SetActive(false);
            }
        }
    }
}