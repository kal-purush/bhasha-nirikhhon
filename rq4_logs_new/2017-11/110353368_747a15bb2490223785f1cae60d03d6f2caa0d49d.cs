using UnityEngine;

class BackgroundScroller : MonoBehaviour
{
    public Camera camera;
    public SpriteRenderer leftBackground;
    public SpriteRenderer rightBackground;

    void Start()
    {
        rightBackground.transform.position = new Vector3(leftBackground.transform.position.x + leftBackground.bounds.size.x, 0, 0);
    }

    /**
     * @todo fix when the camera suddenly moves to an even further location?
     */
    void FixedUpdate()
    {
        float cameraRightEndPosition = camera.transform.position.x + camera.orthographicSize * camera.aspect;
        float cameraLeftEndPosition = camera.transform.position.x - camera.orthographicSize * camera.aspect;
        float backgroundRightEndPosition = rightBackground.transform.position.x + rightBackground.bounds.size.x / 2;
        float backgroundLeftEndPosition = leftBackground.transform.position.x - leftBackground.bounds.size.x / 2;
        if (cameraRightEndPosition > backgroundRightEndPosition)
        {
            SpriteRenderer previousLeftBackground = leftBackground;
            previousLeftBackground.transform.Translate(new Vector3(2 * previousLeftBackground.bounds.size.x, 0, 0));
            leftBackground = rightBackground;
            rightBackground = previousLeftBackground;
        }
        else if (cameraLeftEndPosition < backgroundLeftEndPosition)
        {
            SpriteRenderer previousRightBackground = rightBackground;
            previousRightBackground.transform.Translate(new Vector3(-2 * previousRightBackground.bounds.size.x, 0, 0));
            rightBackground = leftBackground;
            leftBackground = previousRightBackground;
        }
    }
}