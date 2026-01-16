using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

public class CubeCommand : MonoBehaviour
{
    // Called by GazeGestureManager when the user performs a Select gesture
    void OnSelect()
    {
        // If the sphere has no Rigidbody component, add one to enable physics.
        Debug.Log("clicked");
        // SceneManager.LoadScene("testScene");
        transform.Rotate(new Vector3(0, 0, 180)* Time.deltaTime);

    }
   /* void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Debug.Log("clicked");
            transform.Rotate(new Vector3(0, 0, 180)*Time.deltaTime);
        }
    }*/
}