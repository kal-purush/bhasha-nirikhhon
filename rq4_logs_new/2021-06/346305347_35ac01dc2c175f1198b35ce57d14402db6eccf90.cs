using System.Collections;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
using TMPro;

public class ShockTransition : MonoBehaviour
{
    [SerializeField] private float _blackOutDuration;
    [SerializeField] private float _textDuration;
    [SerializeField] private TextMeshProUGUI _text;
    [SerializeField] private Image _bg;

    private void OnEnable() {
        GameManager.Instance.onShockTransition += TransitionWarpper;
    }    
    private void OnDisable() {
        // GameManager.Instance.onShockTransition -= TransitionWarpper;
    }

    private void TransitionWarpper()
    {
        GameManager.Instance.onShockTransition -= TransitionWarpper;
        StartCoroutine(Transition());
    }

    IEnumerator Transition()
    {
        float elapsedTime = 0f;
        Color colorToAdd = new Color(0f, 0f, 0f, 0f);
        while (elapsedTime < _blackOutDuration)
        {
            colorToAdd.a = _bg.color.a - Mathf.Lerp(0f, 1f, elapsedTime / _blackOutDuration);
            _bg.color += colorToAdd;
            elapsedTime += Time.deltaTime;
            yield return null;
        }

        colorToAdd.a = 0f;
        elapsedTime = 0f;
        while (elapsedTime < _textDuration)
        {
            colorToAdd.a = _text.color.a - Mathf.Lerp(0f, 1f, elapsedTime / _blackOutDuration);
            _text.color += colorToAdd;
            elapsedTime += Time.deltaTime;
            yield return null;
        }

        yield return new WaitForSeconds(3f);
        SceneManager.LoadScene(SceneManager.GetActiveScene().name);
    }
}