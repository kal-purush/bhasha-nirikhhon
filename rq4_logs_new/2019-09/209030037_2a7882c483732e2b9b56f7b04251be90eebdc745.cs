using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerInput : MonoBehaviour
{
    public FixedJoystick LeftJoystick;
    public FixedButton JumpButton;
    public FixedTouchField SwipeCamera;
    protected PlayerController Controller;

    protected float CameraAngleX;
    protected float CameraAngleSpeed = 0.2f;

    // Start is called before the first frame update
    void Start()
    {
        Controller = GetComponent<PlayerController>();
    }

    // Update is called once per frame
    void Update()
    {
        Controller.m_Jump = JumpButton.Pressed;
        Controller.HInput = LeftJoystick.Horizontal;
        Controller.VInput = LeftJoystick.Vertical;

        CameraAngleX += SwipeCamera.TouchDist.x * CameraAngleSpeed;

        Camera.main.transform.position = transform.position + Quaternion.AngleAxis(CameraAngleX, Vector3.up) * new Vector3(0, 3, 6);
        Camera.main.transform.rotation = Quaternion.LookRotation(transform.position + Vector3.up * 2f - Camera.main.transform.position, Vector3.up);

    }
}