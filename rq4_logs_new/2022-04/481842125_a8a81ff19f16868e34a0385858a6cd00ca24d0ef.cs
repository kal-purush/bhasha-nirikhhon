using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using InputProvider;

public class FPSCamera : MonoBehaviour
{
    [SerializeField] private Vector2 _rotateSpeed = new Vector2(0.5f, 0.5f);
    [SerializeField] private Vector2 _horizontalRangeInspector = new Vector2(-85, 85);
    [SerializeField] private Vector2 _verticalRangeInspector = new Vector2(-85, 85);

    private Vector2 _horizontalRange;
    private Vector2 _verticalRange;
    private Quaternion _defaultRotation;

    void OnVaridate()
    {
        SetAngleRange();
    }

    void Start()
    {
        _defaultRotation = transform.rotation;
        SetAngleRange();
    }
    
    void Update()
    {
        var inputLookVector = UnityInputProvider.Instance.LookVector();
        if (inputLookVector == Vector2.zero) return;
    
        float horiMouse = inputLookVector.x * _rotateSpeed.x;
        float vertMouse = inputLookVector.y * _rotateSpeed.y;
        transform.Rotate(new Vector3(0, horiMouse, 0), Space.World);
        transform.Rotate(new Vector3(-vertMouse, 0, 0)); // そのままやるとy軸が反転する

        // 回転を範囲内に制限
        float horiAngle = normalizeAngle(transform.eulerAngles.y);
        float vertAngle = normalizeAngle(transform.eulerAngles.x);

        horiAngle = Mathf.Clamp(horiAngle, _horizontalRange.x, _horizontalRange.y);
        vertAngle = Mathf.Clamp(vertAngle, -_verticalRangeInspector.y, -_verticalRangeInspector.x); // y軸反転したほうが直感的になる
        
        transform.rotation = Quaternion.Euler(vertAngle, horiAngle, 0);
    }

    // カメラの初期回転が０じゃないときのための対策
    private void SetAngleRange()
    {
        var horiAngle = _defaultRotation.eulerAngles.y;
        var vertAngle = _defaultRotation.eulerAngles.x;
        _horizontalRange = _horizontalRangeInspector + (Vector2.one * horiAngle);
        _verticalRange = _verticalRangeInspector + (Vector2.one * vertAngle);
        
        _horizontalRange = new Vector2(normalizeAngle(_horizontalRange.x), normalizeAngle(_horizontalRange.y));
        _verticalRange = new Vector2(normalizeAngle(_verticalRange.x), normalizeAngle(_verticalRange.y));
    }

    // -180 ~ 180に正規化
    private float normalizeAngle(float givenAngle)
    {
        return Mathf.Repeat(givenAngle + 180, 360) - 180;
    }
}