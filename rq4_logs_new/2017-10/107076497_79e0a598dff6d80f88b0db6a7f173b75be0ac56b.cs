using UnityEngine.UI;
using UnityEngine;

public class TapManager : MonoBehaviour
{

    [SerializeField]
    private float _distance = 100;

    [SerializeField]
    private Text[] _text = null;

    private Vector2 _Screen;               //スクリーンサイズ（横、縦）

    private GameObject _target = null;

    [SerializeField]
    private bool _DispStatus = false;

    enum TapType
    {
        None = 0,
        SingleTap,
        MultiTap,
    }

    void Start()
    {
        _Screen.x = Screen.width;
        _Screen.y = Screen.height;
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
            MultiTapEvent();
        }

        if (Input.GetMouseButtonDown(0))
        {
            Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);
            RaycastHit hit = new RaycastHit();
            if (Physics.Raycast(ray, out hit, _distance))
            {
                _target = hit.collider.gameObject;
				_target.GetComponent<ObjectBase>().MouseButtonDown();
				_target.GetComponent<Renderer>().material.color = new Color(Random.Range(0.0f, 1.0f),
                                                                        Random.Range(0.0f, 1.0f),
                                                                            Random.Range(0.0f, 1.0f));
			}
        }

        // 選択したオブジェクトの情報を出力する
        if( _target && _DispStatus )
        {
            // TODO:
            var pos = _target.transform.position;

            _text[0].text = "Pos\n" +
                            "x:" + Mathf.Round(_target.transform.localPosition.x).ToString() + 
                            "y:" + Mathf.Round(_target.transform.localPosition.y).ToString() + 
                            "z:" + Mathf.Round(_target.transform.localPosition.z).ToString();
        }
    }

    // シングルタップイベント
    void SingleTapEvent()
    {
        //回転
        var touch = Input.GetTouch(0);
        var ray = Camera.main.ScreenPointToRay(touch.position);
        var hit = new RaycastHit();

        if (Physics.Raycast(ray, out hit, _distance))
        {
            _target = hit.collider.gameObject;
         
            switch (touch.phase)
            {
                case TouchPhase.Began:
                    
                    _target.GetComponent<ObjectBase>().TouchBegan(touch, _Screen);
                    Debug.Log(_target.GetType());
            
                    break;
                case TouchPhase.Moved:
                    Debug.Log("Moved");
                    _target.GetComponent<ObjectBase>().TouchMove(touch);

                    break;
                case TouchPhase.Ended:

                    _target.GetComponent<ObjectBase>().TouchEnded(touch);

                    break;
            }
        }
    }

    // マルチタップイベント
    void MultiTapEvent()
    {
		//TODO: マルチタップの処理を記述する
	}
}