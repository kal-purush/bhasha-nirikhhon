using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WindEmitter : MonoBehaviour {

    [SerializeField] private GameObject _windObject;
    [SerializeField] private GameObject _windEmitterObject;
    private float _timer;

	// Use this for initialization
	void Start () {
        StartCoroutine(BlowWind());
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public IEnumerator BlowWind(){
        //Debug.Log("Blowing Wind");
        _timer = Random.Range(0.2f, 0.5f);
        yield return new WaitForSeconds(_timer);
        Instantiate(_windObject, _windEmitterObject.transform.position, _windEmitterObject.transform.rotation, gameObject.transform);
        StartCoroutine(BlowWind());
    }
}