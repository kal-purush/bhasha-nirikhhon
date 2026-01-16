using UnityEngine;
using UnityEngine.UI;

public class Compass : MonoBehaviour
{
    public RectTransform objectivePointer;
    public float compassWidth = 204f; // The width of your compass background in pixels
    public float pointerMovementScale = 3f; // Adjust this to control how quickly the pointer moves

    private Camera mainCamera;
    private RectTransform compassRect;

    void Start()
    {
        mainCamera = Camera.main;
        compassRect = GetComponent<RectTransform>();
    }

    void Update()
    {
        UpdateObjectivePointer();
    }

    void UpdateObjectivePointer()
    {
        Vector3 objectivePosition = LevelManager.GetCurrentObjectiveLocation();
        if (objectivePosition != Vector3.zero)
        {
            // Calculate the direction to the objective
            Vector3 objectiveDirection = objectivePosition - mainCamera.transform.position;
            objectiveDirection.y = 0; // Ignore vertical difference

            // Calculate the angle between the camera's forward and the objective direction
            float angle = Vector3.SignedAngle(mainCamera.transform.forward, objectiveDirection, Vector3.up);

            // Apply the movement scale
            float scaledAngle = angle * pointerMovementScale;

            // Convert angle to a position on the compass
            float pointerX = (scaledAngle / 180f) * (compassWidth / 2f);

            // Clamp the pointer position to the compass bounds
            pointerX = Mathf.Clamp(pointerX, -compassWidth / 2f, compassWidth / 2f);

            // Set the pointer's position
            objectivePointer.anchoredPosition = new Vector2(pointerX, 0);

            // Make the pointer visible
            objectivePointer.gameObject.SetActive(true);
        }
        else
        {
            // Hide the pointer if there's no valid objective
            objectivePointer.gameObject.SetActive(false);
        }
    }
}