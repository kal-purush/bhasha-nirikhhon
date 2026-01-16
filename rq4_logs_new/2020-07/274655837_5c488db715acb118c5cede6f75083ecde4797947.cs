using System.Runtime.ExceptionServices;
using UnityEngine;
using UnityEngine.UIElements;

public class Movement : MonoBehaviour
{
    // Start is called before the first frame update
    public GameObject window, leftBorder, rightBorder; // окно и границы
    RectTransform rectLeft, rectRight, rectWindow;
    float speedTime = 2f; // Время за которое окно делает полное перемещение с левой границы до правой, разумеется чем меньше это значение тем выше скорость
    float stepScreen, stepWorld; // шаг, для движения за один такт (в экранном режиме и в режиме реального пространтсва)

    Transform leftPos, rightPos, windowPos;
    BoxCollider2D leftBorderCol, rightBorderCol;
    Game game;


    void Start()
    {
        window = gameObject.transform.GetChild(0).gameObject;
        leftBorder = gameObject.transform.GetChild(1).gameObject;
        rightBorder = gameObject.transform.GetChild(2).gameObject;

        game = GameObject.FindGameObjectWithTag("Game").GetComponent<Game>();

        rectLeft = leftBorder.GetComponent<RectTransform>();
        rectRight = rightBorder.GetComponent<RectTransform>();
        rectWindow = window.GetComponent<RectTransform>();

        leftPos = rectLeft.transform;
        rightPos = rectRight.transform;
        windowPos = window.transform;

        leftBorderCol = leftBorder.GetComponent<BoxCollider2D>();
        rightBorderCol = rightBorder.GetComponent<BoxCollider2D>();

        stepScreen = (rectRight.anchorMax.x * Screen.width - rectLeft.anchorMin.x * Screen.width) / (speedTime / 0.02f);
        stepWorld = Camera.main.ScreenToWorldPoint(new Vector2(stepScreen, 0)).x - Camera.main.ScreenToWorldPoint(new Vector2(0, 0)).x;
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        if (game.activeGame)
        {
            rectLeft.SetSizeWithCurrentAnchors(RectTransform.Axis.Horizontal, rectLeft.rect.width - stepScreen);
            leftPos.position = new Vector3(leftPos.position.x - stepWorld / 2, leftPos.position.y, leftPos.position.z);

            rectRight.SetSizeWithCurrentAnchors(RectTransform.Axis.Horizontal, rectRight.rect.width + stepScreen);
            rightPos.position = new Vector3(rightPos.position.x - stepWorld / 2, rightPos.position.y, rightPos.position.z);

            windowPos.position = new Vector3(windowPos.position.x - stepWorld, windowPos.position.y, windowPos.position.z);

            leftBorderCol.size = new Vector2(rectLeft.rect.width, rectLeft.rect.height);
            rightBorderCol.size = new Vector2(rectRight.rect.width, rectRight.rect.height);

            if (rectLeft.rect.width < 0 || rectRight.rect.width < 0)
            {
                stepWorld = -stepWorld;
                stepScreen = -stepScreen;
            }
        }
    }
}