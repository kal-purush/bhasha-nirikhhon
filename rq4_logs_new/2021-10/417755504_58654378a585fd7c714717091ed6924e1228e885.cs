using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;
using System;

/// <summary>スマートフォンパネル</summary>
public class SmartPhonePanelController : MonoBehaviour
{
    EventTrigger m_eventTrigger = null;
    [SerializeField] GameObject m_A_button = null;
    [SerializeField] GameObject m_B_button = null;
    [SerializeField] GameObject m_Right_button = null;
    [SerializeField] GameObject m_Left_button = null;
    [SerializeField] GameObject m_Jump_button = null;

    //各ボタンにeventtriggerを追加して、メソッドを追加します。
    public void EventTriggerAddListeners(Action a_button, Action b_button, Action r_button, Action l_button, Action j_button)
    {
        m_eventTrigger = m_A_button.AddComponent<EventTrigger>();
        EventTrigger.Entry a_entry = new EventTrigger.Entry();
        a_entry.eventID = EventTriggerType.UpdateSelected;
        a_entry.callback.AddListener((x) => a_button());
        m_eventTrigger.triggers.Add(a_entry);

        m_eventTrigger = m_B_button.AddComponent<EventTrigger>();
        EventTrigger.Entry b_entry = new EventTrigger.Entry();
        b_entry.eventID = EventTriggerType.UpdateSelected;
        b_entry.callback.AddListener((x) => b_button());
        m_eventTrigger.triggers.Add(b_entry);

        m_eventTrigger = m_Right_button.AddComponent<EventTrigger>();
        EventTrigger.Entry r_entry = new EventTrigger.Entry();
        r_entry.eventID = EventTriggerType.UpdateSelected;
        r_entry.callback.AddListener((x) => r_button());
        m_eventTrigger.triggers.Add(r_entry);

        m_eventTrigger = m_Left_button.AddComponent<EventTrigger>();
        EventTrigger.Entry l_entry = new EventTrigger.Entry();
        l_entry.eventID = EventTriggerType.UpdateSelected;
        l_entry.callback.AddListener((x) => l_button());
        m_eventTrigger.triggers.Add(l_entry);

        m_eventTrigger = m_Jump_button.AddComponent<EventTrigger>();
        EventTrigger.Entry j_entry = new EventTrigger.Entry();
        j_entry.eventID = EventTriggerType.UpdateSelected;
        j_entry.callback.AddListener((x) => j_button());
        m_eventTrigger.triggers.Add(j_entry);
    }
}