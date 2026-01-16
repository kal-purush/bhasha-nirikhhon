using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI; 

public class AnswerCube : MonoBehaviour
{
    [Header("Physics Settings")]
    private Color startingColor;
    private Color responseColor;
    
    [Header("Progress Settings")]
    public Text progressDisplay;
    private float progress = 0f;

    [Header("Correct Settings")]
    public Text correctDisplay;
    private float correct = 0f;

    [Header("Attempts Settings")]
    public Text attemptDisplay;
    private float attempts = 0f;

    // Start is called before the first frame update
    void Start()
    {
        startingColor = GetComponent<Renderer>().material.color;
        progressDisplay = GetComponent<Text>() as Text;
        correctDisplay = GetComponent<Text>() as Text;
        attemptDisplay = GetComponent<Text>() as Text;
    }

    // Update is called once per frame
    void Update()
    {
        progressDisplay.text = progress.ToString("0");
        correctDisplay.text = attempts.ToString("0");
        attemptDisplay.text = attempts.ToString("0");
    }

    void DestroyGameObject()
    {
        Destroy(gameObject);
    }
    
    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "AnswerDoor")
        {
            if (other.GetComponent<Renderer>().material.color == Color.green)
            {
                responseColor = Color.green;
                progress++;
                Destroy(other.gameObject);
                GetComponent<Renderer>().material.color = startingColor;
                Debug.Log("Next question unlocked!");
            }
            else
            {
                responseColor = Color.red;
                Debug.Log("User attempted to unlock next question");
            }
        }
        else if (other.gameObject.tag == "AnswerPlatform")
        {
            if (other.GetComponent<Renderer>().material.color == Color.green)
            {
                correct++;
            }
            attempts++;
        }
        else
        {
            return;
        }
    }
}