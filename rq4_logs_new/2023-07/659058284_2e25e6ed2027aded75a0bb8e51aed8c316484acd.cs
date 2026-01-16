using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class KeyInputButton : MonoBehaviour
{
    // The target key for this button
    public KeyCode targetKey;

    private Button button;

    private void Start()
    {
        // Get the reference to the Button component
        button = GetComponent<Button>();
        
        // Add a listener to the button's onClick event
        button.onClick.AddListener(OnButtonClick);
    }

    private void Update()
    {
        // Check if the target key is pressed
        if (Input.GetKeyDown(targetKey))
        {
            // Invoke the button's onClick event
            button.onClick.Invoke();
        }
    }

    private void OnButtonClick()
    {
        // This method is called when the button is clicked
        
        // Implement your button click functionality here
        // For example, you can add code to perform specific actions or call other methods

        // Example: Logging the button click
        Debug.Log("Button " + gameObject.name + " clicked!");
    }
}
