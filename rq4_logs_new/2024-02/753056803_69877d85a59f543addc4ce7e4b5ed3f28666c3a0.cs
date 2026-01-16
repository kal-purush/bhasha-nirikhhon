using UnityEngine;
using UnityEngine.UI;
using System.IO;

public class CanvasImageSaver : MonoBehaviour
{
    public Pen pen; // Reference to the Pen script
    private PenState previousPenState; // Previous state of the pen
    private int touchCount = 0; // Number of times the pen touches the paper
    private RawImage rawImage; // Reference to the RawImage component

    private void Start()
    {
        previousPenState = pen.penState; // Initialize the previous pen state
        // Find the RawImage component in children
        rawImage = GetComponentInChildren<RawImage>();
        if (rawImage == null)
        {
            Debug.LogError("RawImage component not found.");
        }
    }

    private void Update()
    {
        // Check if the pen state has changed from touching to not touching
        if (pen.penState == PenState.Touching && previousPenState == PenState.NotTouching)
        {
            touchCount++; // Increment the touch count
            Debug.Log(touchCount);
        }
        else if (pen.penState == PenState.NotTouching && touchCount > 0)
        {
            SaveCanvasImage(touchCount); // Save the canvas image when the pen state changes
        }

        previousPenState = pen.penState;
    }

    private void SaveCanvasImage(int count)
    {
        if (rawImage == null)
        {
            Debug.LogError("RawImage component not found.");
            return;
        }

        // Create a temporary RenderTexture with the same dimensions as the RawImage
        RenderTexture renderTexture = RenderTexture.GetTemporary(
            (int)rawImage.rectTransform.rect.width,
            (int)rawImage.rectTransform.rect.height,
            0,
            RenderTextureFormat.ARGB32
        );

        // Set the target RenderTexture for rendering
        Graphics.SetRenderTarget(renderTexture);

        // Render the RawImage to the RenderTexture
        Graphics.Blit(rawImage.texture, renderTexture);

        // Read the pixels from the RenderTexture
        Texture2D texture = new Texture2D(renderTexture.width, renderTexture.height);
        texture.ReadPixels(new Rect(0, 0, renderTexture.width, renderTexture.height), 0, 0);
        texture.Apply();

        // Release the temporary RenderTexture
        RenderTexture.ReleaseTemporary(renderTexture);

        // Encode the texture as a PNG
        byte[] bytes = texture.EncodeToPNG();

        // Define the file path to save the image with the touch count in the name
        string fileName = $"saved_image_{count}.png";
        string filePath = Path.Combine(Application.dataPath, "Image", fileName);

        // Write the bytes to a file
        File.WriteAllBytes(filePath, bytes);

        Debug.Log($"Image {count} saved to: {filePath}");
    }
}