using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EyelevelCamera : MonoBehaviour {


    private Human _human;
    private Camera _camera;

	// Use this for initialization
	void Start () {

        // 親のスクリプトを取得
        _human = GetComponentInParent<Human>();

        // 最初は表示しない
        GetComponent<Camera>().enabled = false;
	}
	
	// Update is called once per frame
	void Update () {
        if (_human.ready)
        {
            GetComponent<Camera>().enabled = true;
            transform.position = _human.eye_level * 10f;
            Debug.Log(_human.eye_level);
        }
        else {

            GetComponent<Camera>().enabled = false;
        }
	}
}