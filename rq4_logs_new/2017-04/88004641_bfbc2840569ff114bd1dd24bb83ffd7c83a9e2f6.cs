using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerManager : MonoBehaviour {
	public static PlayerManager instance;
	private int myMoney; // 所持金
	private List<MstCharacter> myMentors = new List<MstCharacter>(); //雇ったメンターの一覧
	private int myProductivity; //メンターの生産性の合計
	void Start () {
		// singleton化する
		if (instance == null)
		{
			instance = this;
			DontDestroyOnLoad (instance.gameObject);
		}

		StartCoroutine ("AutomaticProduce");
	}

	// 毎秒で生産性の分の利益を上げるコルーチン
	private IEnumerator AutomaticProduce ()
	{
		while (true)
		{
			myMoney += myProductivity;
			yield return new WaitForSeconds (1f);
		}
    }
}
