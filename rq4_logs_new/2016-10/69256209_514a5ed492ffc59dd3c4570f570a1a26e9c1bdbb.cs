using UnityEngine;
using System.Collections;

public class ExitButton : MonoBehaviour, IInteractableObject
{
    [SerializeField]
    GameObject Highlight;

    bool m_inUse = false;
    bool m_isUsable = true;
    bool m_highlightOnTouch = true;

    public bool highlightOnTouch
    {
        get
        {
            return m_highlightOnTouch;
        }

        set
        {
            m_highlightOnTouch = value;
        }
    }

    public bool inUse
    {
        get
        {
            return m_inUse;
        }

        set
        {
            m_inUse = value;
        }
    }

    public bool isUsable
    {
        get
        {
            return m_isUsable;
        }

        set
        {
            m_isUsable = value;
        }
    }

    public void Interact()
    {
        Application.Quit();
    }

    public void Touch(bool beingTouched)
    {
        if (highlightOnTouch && !inUse)
        {
            Highlight.SetActive(beingTouched);
        }
    }
}