using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LiquidDiluting : MonoBehaviour
{
    public GameObject liquid;
    public Color liquidColour;
    private Renderer _liquidRenderer;
    private Coroutine _mixingCoroutine;

    private void Start()
    {
        Transform liquidChild = transform.Find("Liquid");
        if (liquidChild != null) {
            Debug.Log("Liquid Child found");
            liquid = liquidChild.gameObject;
            _liquidRenderer = liquid.GetComponent<Renderer>();

            if (_liquidRenderer != null) {
                liquidColour = _liquidRenderer.material.color;
                Debug.Log($"Initial liquid color: {liquidColour}");
            } else {
                Debug.LogError("Renderer component not found on liquid.");
            }
        } else {
            Debug.LogError("Liquid Child not found");
        }
    }

    public void MixLiquid(Color colour, float duration)
    {
        if (_mixingCoroutine != null)
        {
            StopCoroutine(_mixingCoroutine);
        }

        Debug.Log($"Begin mixing of {colour} and {liquidColour}");
        _mixingCoroutine = StartCoroutine(MixLiquidRoutine(colour, duration));
    }

    private IEnumerator MixLiquidRoutine(Color colour, float duration)
    {
        if (_liquidRenderer == null)
        {
            Debug.LogError("Renderer is null. Cannot mix colors.");
            yield break;
        }

        Color initialColor = _liquidRenderer.material.color;
        Color targetColour = (initialColor + colour) / 2.0f; // Mix initial and new color
        Debug.Log($"Target colour is: {targetColour}");
        float elapsedTime = 0f;

        while (elapsedTime < duration)
        {
            elapsedTime += Time.deltaTime;
            // Calculate the current color based on the elapsed time
            Color currentColor = Color.Lerp(initialColor, targetColour, elapsedTime / duration);
            _liquidRenderer.material.color = currentColor;
            yield return null;
        }

        // Ensure the final color is set
        _liquidRenderer.material.color = targetColour;
        Debug.Log($"Final color set to: {targetColour}");

        liquidColour = targetColour; // Update liquidColour with the new mixed color
    }
}