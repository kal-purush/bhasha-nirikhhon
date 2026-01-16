using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SettingManager : MonoBehaviour
{
    /// <summary>設定画面のオブジェクト</summary>
    [SerializeField] GameObject m_settingPnel;
    /// <summary>コントロールボタン</summary>
    [SerializeField] GameObject m_button;
    /// <summary>ボタンテキスト</summary>
    [SerializeField] Text m_buttonText;
    
    /// <summary>
    /// 設定ボタン
    /// </summary>
    public void SettingButton()
    {
        m_settingPnel.SetActive(true);
    }

    /// <summary>
    /// 閉じるボタン
    /// </summary>
    public void CloseButton()
    {
        m_settingPnel.SetActive(false);
    }

    /// <summary>
    /// コントロールボタン
    /// </summary>
    public void ControllerButton()
    {
        //プレイヤーを探して操作を変更
        GameObject player = GameObject.Find("Player(Clone)");
        PlayerController playerController = player.GetComponent<PlayerController>();

        switch (playerController.m_controller)
        {
            case PlayerController.ControllerState.Joystick:
                playerController.Acceleration();
                m_buttonText.text = "changeAcceleration";
                break;
            case PlayerController.ControllerState.Acceleration:
                playerController.Joystick();
                m_buttonText.text = "changeJoystick";
                break;
        }
    }
}