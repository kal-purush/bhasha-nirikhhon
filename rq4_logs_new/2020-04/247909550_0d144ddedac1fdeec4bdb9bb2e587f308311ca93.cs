using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Customer : MonoBehaviour
{
	enum FoodReview
	{
		good,
		usually,
		row,
		bad
	}

	int synchroFoodNum = 1;
	Animator animator;
	[SerializeField] bool hasCalorie;
	[SerializeField] bool boss;

    // Start is called before the first frame update
    void Start()
    {
		animator = GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

	void DoAction()
	{
		//食材抽選
	}

	void DoReaction(FoodReview foodReview)
	{
		switch (foodReview)
		{
			case FoodReview.good:
				//アニメーション
				//スコア加算
				//ラッシュゲージ加算
				break;
			case FoodReview.usually:
				//アニメーション
				//スコア加算
				break;
			case FoodReview.row:
				//アニメーション
				//スコア加算
				break;
			case FoodReview.bad:
				//アニメーション
				//スコア加算
				break;
		}

		//クリア判定
		judgeClear();
	}

	void judgeClear()
	{

	}
}