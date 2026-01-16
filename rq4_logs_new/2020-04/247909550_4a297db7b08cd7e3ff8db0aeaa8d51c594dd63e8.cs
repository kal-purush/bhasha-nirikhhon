using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Amida;

public class StageGenerater : MonoBehaviour
{
	//プレハブ
	//線
	[SerializeField] GameObject HorizontalLine;
	[SerializeField] GameObject VerticalLine;
	//油
	[SerializeField] GameObject HighOil;
	[SerializeField] GameObject ModerateOil;
	[SerializeField] GameObject LowOil;
	[SerializeField] GameObject TrashBox;
	//変数名に難あり？
	//ステージデータ
	[SerializeField] Cooking.OilStatus[] oilStatus;
	[SerializeField, Range(1, 8)] int HorizontalLinesNum;
	float maxXPos = 4;
	float minXPos = -7;
	float maxYPos = 4.5f;
	float minYPos = -3;
	//参照パス
	[SerializeField] LineCursol lineCursol;
	[SerializeField] GameManager gameManager;
	[SerializeField] FoodGenerater foodGenerater;
	[SerializeField] ItemGenerater itemGenerater;

	private void Awake()
	{
		GenerateStage();
	}

	// Start is called before the first frame update
	void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

	void GenerateStage()
	{
		float xLength = maxXPos - minXPos;
		float xInterval = 2;
		float yLength = maxYPos - minYPos;
		float yInterval=yLength/(HorizontalLinesNum + 1);

		//▼縦線の生成
		for(int i = 0; i < oilStatus.Length; i++)
		{
			Instantiate(VerticalLine, new Vector3(minXPos + (xInterval * i), 0.5f, 0), Quaternion.identity);
		}

		//▼横線の生成
		GameObject[,] amidaLines = new GameObject[oilStatus.Length - 1, HorizontalLinesNum];

		for(int i = 0; i < oilStatus.Length - 1; i++)
		{
			for(int j = 0; j < HorizontalLinesNum; j++)
			{
				amidaLines[i, j] = Instantiate(HorizontalLine, new Vector3(minXPos + (xInterval * i) + 1, maxYPos - ((yInterval * j) + (i % 2 * yInterval / 2)) - 1, 0), Quaternion.identity);
			}
		}

		lineCursol.AmidaLines = amidaLines;

		//▼油の生成
		GameObject[] oils = new GameObject[oilStatus.Length];
		for (int i = 0; i < oilStatus.Length; i++)
		{
			switch (oilStatus[i])
			{
				case Cooking.OilStatus.high:
					oils[i] = Instantiate(HighOil, new Vector3(minXPos + (xInterval * i), minYPos - 1, 0), Quaternion.identity);
					break;
				case Cooking.OilStatus.moderate:
					oils[i] = Instantiate(ModerateOil, new Vector3(minXPos + (xInterval * i), minYPos - 1, 0), Quaternion.identity);
					break;
				case Cooking.OilStatus.low:
					oils[i] = Instantiate(LowOil, new Vector3(minXPos + (xInterval * i), minYPos - 1, 0), Quaternion.identity);
					break;
				case Cooking.OilStatus.trash:
					oils[i] = Instantiate(TrashBox, new Vector3(minXPos + (xInterval * i), minYPos - 1, 0), Quaternion.identity);
					break;
			}
		}

		gameManager.OilObjs = oils;

		//▼食材生成場所の生成
		//ここGameObjectよりVector３で渡したほうが良い
		GameObject[] foodGeneratePlaces = new GameObject[oilStatus.Length];
		for (int i = 0; i < oilStatus.Length; i++)
		{
			foodGeneratePlaces[i] = Instantiate(new GameObject("foodGeneratePlace"), new Vector3(minXPos + (xInterval * i), maxYPos, 0), Quaternion.identity);
		}

		foodGenerater.GeneratePlaces = foodGeneratePlaces;

		//▼アイテム生成場所の生成
		//ここGameObjectよりVector３で渡したほうが良い
		GameObject[,] itemGeneraterPlaces = new GameObject[oilStatus.Length, HorizontalLinesNum];
		for (int i = 0; i < oilStatus.Length; i++)
		{
			for (int j = 0; j < HorizontalLinesNum; j++)
			{
				itemGeneraterPlaces[i, j] = Instantiate(new GameObject("itemGeneraterPlace"), new Vector3(minXPos + (xInterval * i), maxYPos - (yInterval * j) - 1, 0), Quaternion.identity);
			}
		}

		itemGenerater.GeneratePlaces = itemGeneraterPlaces;
	}
}