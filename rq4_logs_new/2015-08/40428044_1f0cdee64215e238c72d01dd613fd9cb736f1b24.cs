using UnityEngine;
using System.Collections;
using DG.Tweening;

public class MainMapShake : MonoBehaviour {
	public float moveTime = 0.8f;
	public float movePos = 90;
	public float scaleMult = 1.2f;
	public float scaleDuration = 0.8f;
	public float shakeStrength = 10f;
	public float shakeDuration = 0.6f;
	public int shakeTime = 50;

	public Transform[] mapList;

	// Use this for initialization
	void Start () {
		MapSnake ();
	}
	
	// Update is called once per frame
	void Update () {

	}

	void MapSnake()
	{
		//transform.DOLocalMoveY (movePos, moveTime).From().OnComplete(()=>
		//{
			transform.DOPunchScale(new Vector3(scaleMult, scaleMult, scaleMult), scaleDuration, 1, 0).OnComplete(()=>
			{
				transform.parent.DOShakePosition(shakeDuration, shakeStrength, shakeTime, 90, false);
			});
		//});
	}
}