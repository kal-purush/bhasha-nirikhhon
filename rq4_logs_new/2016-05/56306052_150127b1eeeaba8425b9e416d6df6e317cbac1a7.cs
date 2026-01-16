using UnityEngine;

public class HandPositionIndicatorBehavior : MonoBehaviour
{
    public enum Hand
    {
        Left,
        Right
    }

    private MotionDetector motionDetector;

    public Hand hand;

    public void Start()
    {
        motionDetector =
            FindObjectOfType<GameController>()
            .GetComponent<MotionDetector>();
    }

    void Update()
    {
        float palmPosition;
        if (hand == Hand.Left)
        {
            palmPosition = motionDetector.LeftPalmPosition;
        }
        else
        {
            palmPosition = motionDetector.RightPalmPosition;
        }
        transform.position = new Vector3(palmPosition, transform.position.y, transform.position.z);
    }
}