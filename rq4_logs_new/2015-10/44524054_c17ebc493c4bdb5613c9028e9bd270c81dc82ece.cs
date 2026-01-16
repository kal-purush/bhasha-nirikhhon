
using UnityEngine;
using UnityEngine.UI;


public class TextBlinking : MonoBehaviour {

  Text _text;

  [SerializeField]
  float _blinkSpeed = 0.02f;
  float _blinkAlpha = 1.0f;


  void Start() {
    _text = GetComponent<Text>();
  }

  void Update() {
    _text.color = new Color(0f, 0f, 0f, _blinkAlpha);
    _blinkAlpha -= _blinkSpeed;

    if (_blinkAlpha < 0.0f) {
      _blinkAlpha = 0.0f;
      _blinkSpeed = -_blinkSpeed;
    }
    else if (_blinkAlpha > 1.0f) {
      _blinkAlpha = 1.0f;
      _blinkSpeed = -_blinkSpeed;
    }
  }
}