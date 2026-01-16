using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UniRx;
using UnityEngine.UI;

public class Player1Ui : MonoBehaviour
{
    [SerializeField] Player1HP m_player1Hp;
    [SerializeField] Slider m_hpBar;
    int m_maxHp;
    void Start()
    {
        m_hpBar.value = 1;
        m_maxHp = m_player1Hp.hp.Value;
        m_player1Hp.hp.Subscribe(hp => UpdateHP(hp));
    }

    void UpdateHP(int hp)
    {
        m_hpBar.value = hp / (float)m_maxHp;
    }
}