using UnityEngine;


public class PendulumMovment : MonoBehaviour
{
    public GameObject centerOfRotation;

    public float speedOfPendulum;
    public float swingOffset;
    public float swingAngle;
    public bool directionOfSwing;

    private float _angleSum;
    private float _actualangleAngle;


    void Update()
    {
        _actualangleAngle = speedOfPendulum * Time.deltaTime;

        if (directionOfSwing)
        {
            transform.RotateAround(centerOfRotation.transform.position, Vector3.forward, _actualangleAngle);
            
            _angleSum += _actualangleAngle;
            if (_angleSum + swingOffset > swingAngle)
                directionOfSwing = false;
        }
        else
        {
            transform.RotateAround(centerOfRotation.transform.position, -Vector3.forward, _actualangleAngle);
            
            _angleSum -= _actualangleAngle;
            if (_angleSum + swingOffset < -swingAngle)
                directionOfSwing = true;
        }

        if (_angleSum > 360)
            _angleSum -= 360;

        if (_angleSum < -360)
            _angleSum += 360;
    }
}