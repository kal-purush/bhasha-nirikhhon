using UnityEngine;
using TMPro;

public class TextSlideIn : MonoBehaviour
{
    // ????
    public TextMeshProUGUI textMeshPro;

    // ??????
    public float fadeInDuration = 2f;

    // ?????
    private float initialAlpha = 0f;

    void Start()
    {
        // ?????????
        initialAlpha = textMeshPro.alpha;

        // ???????? 0??????
        SetTextAlpha(textMeshPro, 0f);

        // ??????
        StartCoroutine(SlideIn());
    }

    // ????
    private System.Collections.IEnumerator SlideIn()
    {
        // ?????????????
        yield return new WaitForSeconds(1f);

        // ???????
        float textWidth = textMeshPro.preferredWidth;

        // ?????????
        float startX = -(textWidth / 2);

        // ???????????????????
        float elapsedTime = 0f;
        while (elapsedTime < fadeInDuration)
        {
            // ???????
            float alpha = Mathf.Lerp(0f, initialAlpha, elapsedTime / fadeInDuration);

            // ??????
            float x = Mathf.Lerp(startX, 0f, elapsedTime / fadeInDuration);

            // ???????????
            SetTextAlpha(textMeshPro, alpha);
            SetTextPosition(textMeshPro, x);

            // ?????????
            elapsedTime += Time.deltaTime;
            yield return null;
        }

        // ???????????????
        SetTextAlpha(textMeshPro, initialAlpha);

        // ?????????
        SetTextPosition(textMeshPro, 0f);
    }

    // ???????
    private void SetTextAlpha(TextMeshProUGUI textMeshPro, float alpha)
    {
        Color textColor = textMeshPro.color;
        textColor.a = alpha;
        textMeshPro.color = textColor;
    }

    // ??????
    private void SetTextPosition(TextMeshProUGUI textMeshPro, float x)
    {
        textMeshPro.rectTransform.localPosition = new Vector3(x, 0f, 0f);
    }
}
