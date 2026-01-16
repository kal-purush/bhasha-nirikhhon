using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class DirectionArrowChecker : MonoBehaviour
{
    [SerializeField]
    private Image left = null;
    [SerializeField]
    private Image right = null;

    private Sprite leftInactive = null;
    private Sprite leftActive = null;
    private Sprite rightInactive = null;
    private Sprite rightActive = null;

    void Start()
    {
        if(left == null || right == null)
        {
            Debug.LogError("left or right is NULL");
            return;
        }

        leftInactive = left.sprite;
        rightActive = right.sprite;

        leftActive = Resources.Load<Sprite>("Sprites/UI/UI_Direction_Left");
        rightInactive = Resources.Load<Sprite>("Sprites/UI/UI_Direction_Right_inactive");
    }

    public void OnValueChanged(Vector2 pos)
    {
        if (pos.x <= 0f)
            left.sprite = leftInactive;
        else
            left.sprite = leftActive;

        if (pos.x >= 1f)
            right.sprite = rightInactive;
        else
            right.sprite = rightActive;
    }
}