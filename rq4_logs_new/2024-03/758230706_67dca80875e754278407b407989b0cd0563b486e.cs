using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class noKeypnel : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }
    public void OnBackButtonClick()
    {
        Debug.Log("Back button clicked"); // Add this line for debugging
                                          // Deactivate the code panel
        gameObject.SetActive(false);

        // Resume the game by setting the time scale back to 1
        Time.timeScale = 1f;
    }
}

