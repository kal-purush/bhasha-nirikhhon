using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MainMenuParallax : MonoBehaviour
{
    public Transform[] backgroundPairs;  // Array of pairs of backgrounds to be parallaxed.
    public float[] moveSpeeds;           // Speed at which each background moves.
    
    private SpriteRenderer[][] spriteRenderers; // To store SpriteRenderer components of each background pair.

    private void Start()
    {
        if (backgroundPairs.Length != moveSpeeds.Length)
        {
            Debug.LogError("Ensure that each background pair has a corresponding move speed.");
            this.enabled = false; // Disable the script to prevent errors.
            return;
        }

        spriteRenderers = new SpriteRenderer[backgroundPairs.Length][];

        for (int i = 0; i < backgroundPairs.Length; i++)
        {
            spriteRenderers[i] = backgroundPairs[i].GetComponentsInChildren<SpriteRenderer>();
            
            if (spriteRenderers[i].Length != 2)
            {
                Debug.LogError("Each background pair should consist of two identical sprites.");
                this.enabled = false; // Disable the script to prevent further errors.
                return;
            }
        }
    }

    private void Update()
    {
        for (int i = 0; i < backgroundPairs.Length; i++)
        {
            foreach (var spriteRenderer in spriteRenderers[i])
            {
                // Move the background sprite
                spriteRenderer.transform.Translate(Vector3.left * moveSpeeds[i] * Time.deltaTime);

                // Check if the sprite has moved completely out of view and reposition it to create a seamless loop
                if (spriteRenderer.transform.position.x <= -spriteRenderer.bounds.size.x)
                {
                    Vector3 backgroundShift = new Vector3(spriteRenderer.bounds.size.x * 2f, 0, 0);
                    spriteRenderer.transform.position += backgroundShift;
                }
            }
        }
    }
}