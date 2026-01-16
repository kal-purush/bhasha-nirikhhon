using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MakeButtonActiveAfterXSeconds : MonoBehaviour
{
    Button button;
    [SerializeField] float secondsBeforeEnabling;
    private void Awake()
    {
        button = GetComponent<Button>();
    }

    private void Start()
    {
        button.enabled = false;
    }

    private void OnEnable()
    {
        Invoke("EnableButton", secondsBeforeEnabling);
    }

    private void OnDisable()
    {
        button.enabled = false;
    }

    private void EnableButton()
    {
        button.enabled = true;
    }
}