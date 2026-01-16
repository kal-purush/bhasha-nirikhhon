using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using DG.Tweening;

public class EmailInputAgent : MonoBehaviour
{
    [SerializeField] InputField _inputField;
    [SerializeField] Text _textComponent;


    int _lastSize = 0;

    public void DoTextChanged(string str) {

        int fontLength = _inputField.text.Length;
        _lastSize = fontLength;

        //  当字符数大于17时，进行调整,最大的是50
        if (fontLength >= 18 && fontLength < 36)
        {
            _textComponent.GetComponent<RectTransform>().DOAnchorPos(new Vector2(-5, 0), 0.2f);
        }
        else if (fontLength > 36) {
            _textComponent.GetComponent<RectTransform>().DOAnchorPos(new Vector2(-305, 0), 0.2f);
        }
        else
        {
            _textComponent.GetComponent<RectTransform>().DOAnchorPos(new Vector2(309, 0), 0.2f);
        }
    }


}