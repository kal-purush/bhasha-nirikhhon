using System.Collections;
using DG.Tweening;
using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class GameClearAnimation : MonoBehaviour
{
    [SerializeField] private TMP_Text _text;
    [SerializeField] private string _message;
    [SerializeField] private float _gameclearDelaytime;
    [SerializeField] private float _starImageDelaytime;
    [SerializeField] private Image[] _starImage;

    public IEnumerator GameClearTextAnimation()
    {
        for (int i = 0; i < _message.Length; i++)
        {
            _text.text += _message[i];
            //_text.text
            yield return new WaitForSeconds(_gameclearDelaytime);
        }
    }

    public IEnumerator StarImageAnimationStart()
    {
        for(int i = 0; i < _starImage.Length; i++)
        {
            StartCoroutine(StarImageAnimation(_starImage[i]));
            yield return new WaitForSeconds(_starImageDelaytime);
        }
    }

    public IEnumerator StarImageAnimation(Image image)
    {
        yield return null;
        var sequence = DOTween.Sequence();
        sequence.Append(image.GetComponent<RectTransform>().DOScale(new Vector3(1.2f, 1.2f, 1.2f), 0.5f))
                .Append(image.GetComponent<RectTransform>().DOScale(new Vector3(1f, 1f, 1f), 0.3f));
    }

    //IEnumerator AnimationChangeScale(int index)
    //{
    //    TMP_TextInfo textInfo = _text.textInfo;
    //    var charInfo = textInfo.characterInfo[index];
    //    yield return null;
    //}
}