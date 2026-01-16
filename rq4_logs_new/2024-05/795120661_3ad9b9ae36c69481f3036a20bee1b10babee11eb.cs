using System;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

[RequireComponent(typeof(Button))]
public class CustomButton : UIBehaviour
{
    Button _button;

    Button Button
    {
        get
        {
            if (_button == null) _button = GetComponent<Button>();
            return _button;
        }
    }

    public bool Interactable
    {
        get => Button.interactable;
        set => Button.interactable = value;
    }

    public void ChangeEnableState(bool enableState)
    {
        Button.enabled = enableState;
    }

    public void AddListener(Action action)
    {
        Button.onClick.AddListener(() => { action(); });
    }

    public void AddListener<T>(Action<T> action, T value)
    {
        Button.onClick.AddListener(() => { action(value); });
    }

    public void ClearListeners()
    {
        Button.onClick.RemoveAllListeners();
    }
}