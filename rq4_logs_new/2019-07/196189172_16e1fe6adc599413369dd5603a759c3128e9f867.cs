using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MessageBoxAgent : MonoBehaviour
{
    [SerializeField] Text _message;
    [SerializeField] float durtime = 2f;

    private bool _show = false;
    private bool _showTemp = false;


    public void UpdateMessage(string message) {
        gameObject.SetActive(true);
        _message.text = message;
    }

    public void UpdateMessageTemp(string message)
    {
        _showTemp = true;
        gameObject.SetActive(true);
        _message.text = message;

        StartCoroutine(ShowMessage());

    }

    public void Close() {
        if (!_showTemp) {
            gameObject.SetActive(false);
        }
    }

    void Update() {

    }


    IEnumerator ShowMessage()
    {
        yield return new WaitForSeconds(durtime);
        _showTemp = false;
        gameObject.SetActive(false);
    }


}