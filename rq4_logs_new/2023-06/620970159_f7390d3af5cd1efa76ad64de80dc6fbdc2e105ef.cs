using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Cooldown : MonoBehaviour
{
    public float transitionTime = 1f; // Transition time in seconds
    public float targetAlpha = 0f; // Target alpha value you want to achieve

    private Image spriteRenderer;
    private Color initialColor;
    private Color targetColor;
    private float startTime;
    private bool isFading = false; // Flag to indicate if the fade-out process is active

    private void Start()
    {
        spriteRenderer = GetComponent<Image>();
        initialColor = spriteRenderer.color;
        targetColor = new Color(initialColor.r, initialColor.g, initialColor.b, targetAlpha);
        startTime = Time.time;
    }

    private void Update()
    {
        // Calculate the elapsed time since the start of the transition
        float elapsedTime = Time.time - startTime;

        // Calculate the normalized time fraction
        float timeFraction = Mathf.Clamp01(elapsedTime / transitionTime);

        // Determine the target color based on the fade-out process
        Color target = isFading ? targetColor : initialColor;

        // Interpolate between the initial color and the target color using the time fraction
        Color newColor = Color.Lerp(initialColor, target, timeFraction);

        // Check if the "K" key is pressed to start the fade-out process
        if (Input.GetKeyDown(KeyCode.K) && spriteRenderer.color.a == 0f)
        {
            // Set the initial color with alpha value of 1 (fully opaque)
            initialColor = new Color(initialColor.r, initialColor.g, initialColor.b, 1f);
            StartFadeOut();
        }

        // Update the color of the Sprite Renderer
        spriteRenderer.color = newColor;
    }

    private void StartFadeOut()
    {
        // Start the fade-out process
        isFading = true;
        startTime = Time.time; // Reset the start time for smooth transition
    }
}