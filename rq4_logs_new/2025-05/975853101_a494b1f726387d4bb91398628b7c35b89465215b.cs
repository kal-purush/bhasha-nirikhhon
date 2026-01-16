using UnityEngine;
using TMPro;

public class ScorePopup : MonoBehaviour
{
    public float floatUpDistance = 50f;
    public float duration = 1f;

    private Vector3 startPos;
    private Vector3 endPos;
    private float elapsedTime = 0f;
    private TextMeshProUGUI text;

    void Start()
    {
        text = GetComponent<TextMeshProUGUI>();
        startPos = transform.position;
        endPos = startPos + new Vector3(0, floatUpDistance, 0);
    }

    void Update()
    {
        elapsedTime += Time.deltaTime;
        float t = elapsedTime / duration;

        transform.position = Vector3.Lerp(startPos, endPos, t);

        if (text != null)
        {
            text.alpha = 1 - t;
        }

        if (t >= 1f)
        {
            Destroy(gameObject);
        }
    }

    public void SetText(string value, Color color)
    {
        if (text == null) text = GetComponent<TextMeshProUGUI>();
        text.text = value;
        text.color = color;
    }
}