using UnityEngine;
using UnityEngine.UI; 

public class Attempts : MonoBehaviour
{
    public Text attemptDisplay;
    public Transform platform;
    private Color baseColor;
    private float attempts = 0f;
    private float check = 0f;

    void Start()
    {
        attemptDisplay = GetComponent<Text>() as Text;
        baseColor = platform.GetComponent<Renderer>().material.color;
    }
    
    public void Update()
    {
        if (platform.GetComponent<Renderer>().material.color != baseColor && check == 0)
        {
            attempts += 1;
            check += 1;
        }
        attemptDisplay.text = attempts.ToString("0");
    }
}