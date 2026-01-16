/*****
 *  NOT DONE
 *  DO NOT USE
 * */
using UnityEngine;
using System.Collections;

public class NextBlockCameraManager : MonoBehaviour {

	private GameObject nextBlock;

	// Use this for initialization
	void Start () {
		nextBlock = GameManager.instance.getTower ().createBlock();
		nextBlock.GetComponent<Block>().snapping = false;
		nextBlock.transform.position = transform.position + (transform.forward * 2) + new Vector3(0f, -6f, 0f);
	}

	GameObject getNextBlock()
	{
		return nextBlock;
	}

	// Update is called once per frame
	void Update () {
		if (nextBlock)
		{
		}
	}
}