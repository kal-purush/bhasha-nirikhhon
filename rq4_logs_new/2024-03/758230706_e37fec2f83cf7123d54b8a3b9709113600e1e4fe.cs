using UnityEngine;
using TMPro;

public class codePanel : MonoBehaviour
{
    [SerializeField]
    TMP_InputField codeInputField;
    string enteredCode = "";

    // Define your correct code here
    string correctCode = "12345";

    // Start is called before the first frame update
    void Start()
    {
        // Set up a listener for the input field's value changed event
        codeInputField.onValueChanged.AddListener(OnCodeValueChanged);
    }

    void OnCodeValueChanged(string newValue)
    {
        // Update the entered code
        enteredCode = newValue;

        // Check if the entered code matches the correct code
        if (enteredCode == correctCode)
        {
            // If the code is correct, perform actions like opening a door
            player_movement.isDoorOpen = true;
        }
    }

    public void AddDigit(string digit)
    {
        // Append the entered digit to the current code
        enteredCode += digit;

        // Limit the length of the code to avoid too many digits
        if (enteredCode.Length >= 6)
        {
            enteredCode = ""; // Reset the code if it exceeds 6 digits
        }

        // Update the input field text
        codeInputField.text = enteredCode;
    }
}