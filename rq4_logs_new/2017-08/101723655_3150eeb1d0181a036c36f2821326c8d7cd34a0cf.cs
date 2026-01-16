using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Stage : MonoBehaviour {

    private int[][] MapChip = new int[][]
    {
        new int[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
        new int[]{1,2,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        //10
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1},
        new int[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
    };

    public GameObject[] obj;
    private GameObject Empty;
    //ハコ、置き場、壁、無
    //プレイヤー

	// Use this for initialization
	void Start () {
        Empty = GameObject.Find("Stage");
        for (int i = 0; i < 15; i++)
        {
            for (int j = 0; j < 20; j++)
            {
                GameObject chip=null;
                switch (MapChip[i][j])
                {
                    case 0:chip = Instantiate(obj[0], new Vector2((j * 32) - 320 + 16, i * -32 + 240 - 16), Quaternion.identity); break;
                    case 1: chip = Instantiate(obj[1], new Vector2((j * 32) - 320 + 16, i * -32 + 240 - 16), Quaternion.identity); break;
                    case 2: chip = Instantiate(obj[2], new Vector2((j * 32) - 320 + 16, i * -32 + 240 - 16), Quaternion.identity); break;
                    case 3: chip = Instantiate(obj[3], new Vector2((j * 32) - 320 + 16, i * -32 + 240 - 16), Quaternion.identity); break;
                }
                chip.transform.parent = Empty.transform;
            }
        }
    }
	
	// Update is called once per frame
	void Update () {
		
	}
}