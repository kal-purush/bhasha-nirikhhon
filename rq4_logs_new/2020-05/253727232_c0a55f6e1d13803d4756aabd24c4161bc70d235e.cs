using UnityEngine;

public class Rotate : MonoBehaviour
{
    public GameObject note;
    public float rotationSpeedModifier = 500.0f;

    enum State { Init = 0, Rotated = 1, RotatingForward = 2, RotatingBackward = 3 }
    private State state = State.Init;

    void Update()
    {
        if (state == State.RotatingForward)
        {
            // If the rotation of the note in x axis is >= 180
            if (gameObject.transform.eulerAngles.x >= 180.0f)
            {
                // Set the rotation of the note
                gameObject.transform.eulerAngles = new Vector3(0.0f, 0.0f, 90.0f);
                // Set the state to rotated
                state = State.Rotated;
            }
            else
            {
                // Rotating the note forward
                note.transform.Rotate(
                    new Vector3(1f, 1f, 0f),
                    rotationSpeedModifier * Time.deltaTime
                );
            }
        }
        else if (state == State.RotatingBackward)
        {
            // If the rotation of the note in y axis is <= 180
            if (gameObject.transform.eulerAngles.x >= 180.0f)
            {
                // Set the rotation of the note
                gameObject.transform.eulerAngles = new Vector3(0.0f, 180.0f, 0.0f);
                // Set the state to rotated
                state = State.Init;
            }
            else
            {
                // Rotating the note forward
                note.transform.Rotate(
                    new Vector3(-1f, -1f, 0f),
                    rotationSpeedModifier * Time.deltaTime
                );
            }
        }
    }

    public void RotateAround()
    {
        // If note is in initial state
        if (state == State.Init)
            state = State.RotatingForward;
        // If note is in rotated state
        else if (state == State.Rotated)
            state = State.RotatingBackward;
    }
}