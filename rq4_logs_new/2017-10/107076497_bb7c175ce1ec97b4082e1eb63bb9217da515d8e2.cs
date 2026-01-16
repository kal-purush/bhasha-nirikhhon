using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TapManager : MonoBehaviour
{
    [SerializeField]
    private float _rate = 1;
    private float _rot = -90;

    [SerializeField]
    private float _distance = 100;

    private bool _isTouches = false;

    [SerializeField]
    private float _rotationSpeed;

    [SerializeField]
    private float _minAngle = 0.0f,
                  _maxAngle = 90.0f;

    private Vector2 _startLocation;     //タップ開始時にのポジション
    private Quaternion _startRotation;  //タップ開始時の回転角度

    private Vector2 _velocity;          //移動量
    private float _width,               //スクリーンサイズ（横、縦）
                  _height;

    enum TapType
    {
        None = 0,
        SingleTap,
        MultiTap,
    }

    void Start()
    {
        _width = Screen.width;
        _height = Screen.height;
    }

    void Update()
    {
        if (Input.touchCount == 1)
        {
            // シングルタップ
            SingleTapEvent();
        }
        else if (Input.touchCount >= 2)
        {
            // マルチタップ
        }

        if (Input.GetMouseButtonDown(0))
        {
            Debug.Log("タップされました");
            
            Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
            RaycastHit hit = new RaycastHit();
            if (Physics.Raycast(ray, out hit, _distance))
            {
                var obj = hit.collider.gameObject;
                obj.GetComponent<Renderer>().material.color = new Color(Random.Range(0.0f, 1.0f),
                                                                        Random.Range(0.0f, 1.0f),
                                                                        Random.Range(0.0f, 1.0f));
                Debug.Log(obj);
            }
        }
    }

    void SingleTapEvent()
    {
        //回転
        var touch = Input.GetTouch(0);
        var ray = Camera.main.ScreenPointToRay(touch.position);
        var hit = new RaycastHit();
        if (Physics.Raycast(ray, out hit, _distance))
        {
            var obj = hit.collider.gameObject;
            if (touch.phase == TouchPhase.Began)
            {
                _startLocation = touch.position;
                _startRotation = obj.transform.rotation;
            }
            else if (touch.phase == TouchPhase.Moved)
            {
                _velocity.x = (touch.position.x - _startLocation.x) / _width;
                _velocity.y = (touch.position.y - _startLocation.y) / _height;
                obj.transform.rotation = _startRotation;
                obj.transform.Rotate(new Vector3(0, (_rot * _rate) * _velocity.x, 0), Space.World);
            }
        }
    }

    void MultiTapEvent()
    {
		//TODO: マルチタップの処理を記述する
	}
}