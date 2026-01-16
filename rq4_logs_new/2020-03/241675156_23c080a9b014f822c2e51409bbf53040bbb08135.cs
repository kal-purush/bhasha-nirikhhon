using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// This is just a dummy script intented to be attached to the current root game hierarchy, so it makes sure every element is active when 
/// the game starts execution and all scripts will work properly, pointing to a valid reference. 
/// During development, it is more comfortable having the UI Menu no being rendered in both Scene screen and Game screen, (except of course when
/// developers are actually designing the UI)
/// </summary>
public class Script_ActivateGame : MonoBehaviour
{
    [Tooltip("A refence to the World gameObject in the scene")]
    [SerializeField] GameObject m_World;
    [Tooltip("A refence to the Menu gameObject in the scene")]
    [SerializeField] GameObject m_Menu;
    [Tooltip("A refence to the UI gameObject in the scene")]
    [SerializeField] GameObject m_UI;

    void Awake()
    {
        m_World.SetActive(true);
        m_Menu.SetActive(true);
        m_UI.SetActive(true);
    }
}