using UnityEngine;
using UnityEngine.InputSystem;
public class Cleanable : MonoBehaviour
{

    [SerializeField] private Texture2D DirtMaskTextureBase;
    [SerializeField] private Texture2D DirtBrush;
    [SerializeField] private Material Material;

    private GameObject Player;

    private Texture2D DirtMaskTexture;

    private Vector2Int LastPaintPixelPosition;

    private Vector3 playerPosition;

    private float DirtAmountTotal;
    private float DirtAmount;

    private bool IsCleanable;

    public void OnClean(InputAction.CallbackContext context)
    {
        if (!context.started) return;

        IsCleanable = true;
    }

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
    }

    private void Update()
    {
        // Get the player's position
        Player = GameObject.Find("Mop_Char_Pref(Clone)");
        playerPosition = Player.transform.position;

        // Check if the player is within a certain distance of the object
        float distance = Vector3.Distance(transform.position, playerPosition);
        if (distance <= 2f) // Adjust the distance as needed
        {
            // Call the Clean() method
            Clean();
        }
    }
    private void Clean()
    {
        if (GetDirtAmount() <= 0)
        {
            gameObject.SetActive(false);
        }
        //UIText.text = GetDirtAmount() + "%";
        if (Physics.Raycast(playerPosition, -Vector3.up, out RaycastHit raycastHit))
        {
            Vector2 textureCoord = raycastHit.textureCoord;

            int pixelX = (int)(textureCoord.x * DirtMaskTexture.width);
            int pixelY = (int)(textureCoord.y * DirtMaskTexture.height);

            Vector2Int paintPixelPosition = new Vector2Int(pixelX, pixelY);
            //Debug.Log("UV: " + textureCoord + "; Pixels: " + paintPixelPosition);

            int paintPixelDistance = Mathf.Abs(paintPixelPosition.x - LastPaintPixelPosition.x) + Mathf.Abs(paintPixelPosition.y - LastPaintPixelPosition.y);
            int maxPaintDistance = 0;
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
                    int pixelXPos = pixelXOffset + x;
                    int pixelYPos = pixelYOffset + y;

                    if (pixelXPos >= 0 && pixelXPos < DirtMaskTexture.width && pixelYPos >= 0 && pixelYPos < DirtMaskTexture.height)
                    {
                        Color pixelDirt = DirtBrush.GetPixel(x, y);
                        Color pixelDirtMask = DirtMaskTexture.GetPixel(pixelXPos, pixelYPos);

                        float removedAmount = pixelDirtMask.g - (pixelDirtMask.g * pixelDirt.g);
                        DirtAmount -= removedAmount;

                        DirtMaskTexture.SetPixel(
                            pixelXPos,
                            pixelYPos,
                            new Color(0, pixelDirtMask.g * pixelDirt.g, 0)
                        );
                    }
                }
            }
            DirtMaskTexture.Apply();
        }
    }
    private float GetDirtAmount()
    {
        return Mathf.RoundToInt(DirtAmount / DirtAmountTotal * 100);
    }

}