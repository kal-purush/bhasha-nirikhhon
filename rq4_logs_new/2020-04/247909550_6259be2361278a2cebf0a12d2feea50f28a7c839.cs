using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Amida;

public class Oil : MonoBehaviour
{
	[SerializeField] GameObject GameManager;
	GameManager gm;
	[SerializeField] Cooking.OilTemp oilTemp;

    // Start is called before the first frame update
    void Start()
    {
		gm = GameManager.GetComponent<GameManager>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

	private void OnTriggerEnter2D(Collider2D collision)
	{
		//▼食材なら揚げ物としてGameManagerに渡す
		if (collision.gameObject.tag == "food")
		{
			Food food;
			food = collision.gameObject.GetComponent<Food>();
			FriedFood friedFood;
			friedFood = food.DoFry(oilTemp);
			gm.MadeFriedFood = friedFood;
			Destroy(collision.gameObject);
		}
	}
}