using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class DragMover : MonoBehaviour {

    /// <summary>
    /// 自分のRectTransform
    /// </summary>
    RectTransform myTransform = new RectTransform();

    /// <summary>
    /// 前フレームのマウス位置
    /// </summary>
    Vector3 prevMousePosition = Vector3.zero;

    /// <summary>
    /// ドラッグ中かどうか
    /// </summary>
    bool isDragging = false;

    void Awake()
    {
        myTransform = GetComponent<RectTransform>();
    }

	// Use this for initialization
	void Start () {
        MousePositionUpdate();
	}
	
	// Update is called once per frame
	void Update () {

        // ドラッグ状態更新
        DragStateUpdate();

        // ドラッグ中の操作
        if ( isDragging )
        {
            // 位置更新
            PositionUpdate();
        }

        // マウス座標更新
        MousePositionUpdate();
    }

    /// <summary>
    /// マウスが自分の上にいるかどうか
    /// </summary>
    /// <returns>
    /// true...マウスが自分の上に重なっている
    /// false...マウスが自分の上に重なっていない
    /// </returns>
    private bool IsOnMouse()
    {
        // 自分の範囲
        Rect rect = new Rect( myTransform.position.x, myTransform.position.y, myTransform.rect.width, myTransform.rect.height );

        // マウスの座標を取得
        Vector2 mousePosition = new Vector2( Input.mousePosition.x, Input.mousePosition.y );

        // マウスの座標が自分の範囲内に居るかチェック
        if( rect.position.x <= mousePosition.x && mousePosition.x <= rect.position.x + rect.width &&
            rect.position.y <= mousePosition.y && mousePosition.y <= rect.position.y + rect.height )
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// ドラッグ状態の更新・設定
    /// </summary>
    private void DragStateUpdate()
    {
        if (IsOnMouse() && Input.GetMouseButtonDown(0))
        {// マウスが自分の上にあり、マウス左ボタンが押された場合
            isDragging = true;//!< ドラッグ中にする
            MousePositionUpdate();//!< 一応、位置更新
        }
        if (isDragging && Input.GetMouseButtonUp(0))
        {// ドラッグ中にマウス左ボタンから指が離された場合...
            isDragging = false;// ドラッグ中を解除する
        }
    }

    /// <summary>
    /// 画像の座標更新
    /// </summary>
    private void PositionUpdate()
    {
        if(!myTransform) return;

        myTransform.position = myTransform.position + (Input.mousePosition - prevMousePosition);
    }

    /// <summary>
    /// マウス座標を更新
    /// </summary>
    private void MousePositionUpdate()
    {
        // 過去のマウスの座標を更新
        prevMousePosition = Input.mousePosition;
    }

}