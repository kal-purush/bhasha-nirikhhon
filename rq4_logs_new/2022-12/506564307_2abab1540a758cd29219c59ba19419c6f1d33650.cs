using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MyHand : MonoBehaviour
{
    [SerializeField]
    private GameObject _skin;
    [SerializeField]
    private float _timeToHideHands;
    [SerializeField]
    private float _miniHandMovement;

    private float _staticTime = 0.0f;

    private Vector3 _previousPosition;

    // Start is called before the first frame update
    void Start()
    {
        _previousPosition = transform.position;
    }

    // Update is called once per frame
    void Update()
    {
        if ((transform.position - _previousPosition).magnitude < _miniHandMovement){
            _staticTime += Time.deltaTime;
        }else{
            _staticTime = 0.0f;
        }
        _previousPosition = transform.position;

        if (_staticTime > _timeToHideHands){
            _skin.SetActive(false);
        }else{
            _skin.SetActive(true);
        }
    }
}