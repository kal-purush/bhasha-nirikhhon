using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AIMovingTargetController : MonoBehaviour {

    [SerializeField] private Transform _posnOne;
    [SerializeField] private Transform _posnTwo;
    [SerializeField] private Transform _currentPosn;
    [SerializeField] private GameObject _aiAgent;
    private bool _atPosnOne=true;

    void Start () {
        _currentPosn.position = _posnOne.position;

    }
	
	void Update () {
		
	}

    private void OnTriggerEnter(Collider other)
    {
        Debug.Log("Tiggered by " + other.name);
        if (other.gameObject == _aiAgent.gameObject){
            TogglePosition();
        }
    }

    private void TogglePosition(){
        if (_atPosnOne){
            _currentPosn.position = _posnTwo.position;
            _atPosnOne = false;
        }
        else {
            _currentPosn.position = _posnOne.position;
            _atPosnOne = true;
        }
    }
}