using UnityEngine;

public class TestObject : ObjectBase
{
	[SerializeField]
	private float _rate = 1;
	private float _rot = -90;

	private Vector2 _startLocation;     //タップ開始時にのポジション
	private Quaternion _startRotation;  //タップ開始時の回転角度

	private Vector2 _velocity;          //移動量

    private Touch _touch;
    private Vector2 _ScreenSize;

	public override void TouchBegan(Touch touch, Vector2 screenSize)
	{
        base.TouchBegan(touch, screenSize);
        Debug.Log(this.name);
        Debug.Log("タッチしました");

        _touch = touch;
        _ScreenSize = screenSize;

		_startLocation = touch.position;
		_startRotation = this.transform.rotation;
	}

	public override void MouseButtonDown()
	{
		base.MouseButtonDown();
	}

	public override void TouchMove(Touch touch)
	{
        base.TouchMove(touch);

		_velocity.x = (touch.position.x - _startLocation.x) / _ScreenSize.x;
		_velocity.y = (touch.position.y - _startLocation.y) / _ScreenSize.y;
		this.transform.rotation = _startRotation;
		this.transform.Rotate(new Vector3(0, (_rot * _rate) * _velocity.x, 0), Space.World);
        Debug.Log(touch.position);
        Debug.Log("Moved");
    }

	public override void TouchEnded(Touch touch)
	{
        base.TouchEnded(touch);
        Debug.Log("TouchEnded");
	} 
}