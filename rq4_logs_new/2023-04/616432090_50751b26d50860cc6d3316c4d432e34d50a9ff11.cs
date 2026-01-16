using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
public class Cleanable : MonoBehaviour
{

    [SerializeField] private Texture2D DirtMaskTextureBase;
    [SerializeField] private Texture2D DirtBrush;
    [SerializeField] private Material Material;
    //[SerializeField] private TextMeshProUGUI UIText;

    private Texture2D DirtMaskTexture;
    private float DirtAmountTotal;
    private float DirtAmount;
    private Vector2Int LastPaintPixelPosition;

    private void Awake()
    {
        DirtMaskTexture = new Texture2D(DirtMaskTextureBase.width, DirtMaskTextureBase.height);
        DirtMaskTexture.SetPixels(DirtMaskTextureBase.GetPixels());
        DirtMaskTexture.Apply();
        Material.SetTexture("_DirtMask_TX", DirtMaskTexture);

        DirtAmountTotal = 0f;
        for (int x = 0; x < DirtMaskTextureBase.width; x++)
        {
            for (int y = 0; y < DirtMaskTextureBase.height; y++)
            {
                DirtAmountTotal += DirtMaskTextureBase.GetPixel(x, y).g;
            }
        }
        DirtAmount = DirtAmountTotal;

        //FunctionPeriodic.Create(() => {
        //    UIText.text = Mathf.RoundToInt(GetDirtAmount() * 100f) + "%";
        //}, .03f);
    }

    private void Update()
    {
        if (Input.GetMouseButton(0))
        {
            if (Physics.Raycast(Camera.main.ScreenPointToRay(Input.mousePosition), out RaycastHit raycastHit))
            {
                Vector2 textureCoord = raycastHit.textureCoord;

                int pixelX = (int)(textureCoord.x * DirtMaskTexture.width);
                int pixelY = (int)(textureCoord.y * DirtMaskTexture.height);

                Vector2Int paintPixelPosition = new Vector2Int(pixelX, pixelY);
                //Debug.Log("UV: " + textureCoord + "; Pixels: " + paintPixelPosition);

                int paintPixelDistance = Mathf.Abs(paintPixelPosition.x - LastPaintPixelPosition.x) + Mathf.Abs(paintPixelPosition.y - LastPaintPixelPosition.y);
                int maxPaintDistance = 7;
                if (paintPixelDistance < maxPaintDistance)
                {
                    // Painting too close to last position
                    return;
                }
                LastPaintPixelPosition = paintPixelPosition;

                /*
                // Paint Square in Dirt Mask
                int squareSize = 32;
                int pixelXOffset = pixelX - (dirtBrush.width / 2);
                int pixelYOffset = pixelY - (dirtBrush.height / 2);

                for (int x = 0; x < squareSize; x++) {
                    for (int y = 0; y < squareSize; y++) {
                        dirtMaskTexture.SetPixel(
                            pixelXOffset + x,
                            pixelYOffset + y,
                            Color.black
                        );
                    }
                }
                //*/
                int pixelXOffset = pixelX - (DirtBrush.width / 2);
                int pixelYOffset = pixelY - (DirtBrush.height / 2);

                for (int x = 0; x < DirtBrush.width; x++)
                {
                    for (int y = 0; y < DirtBrush.height; y++)
                    {
                        Color pixelDirt = DirtBrush.GetPixel(x, y);
                        Color pixelDirtMask = DirtMaskTexture.GetPixel(pixelXOffset + x, pixelYOffset + y);

                        float removedAmount = pixelDirtMask.g - (pixelDirtMask.g * pixelDirt.g);
                        DirtAmount -= removedAmount;

                        DirtMaskTexture.SetPixel(
                            pixelXOffset + x,
                            pixelYOffset + y,
                            new Color(0, pixelDirtMask.g * pixelDirt.g, 0)
                        );
                    }
                }
                DirtMaskTexture.Apply();
            }
        }

    }

    private float GetDirtAmount()
    {
        return this.DirtAmount / DirtAmountTotal;
    }

}