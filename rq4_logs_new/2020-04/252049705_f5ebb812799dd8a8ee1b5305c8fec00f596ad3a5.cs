using System.Collections;
using System.Collections.Generic;
using UnityEngine;


/// <summary>
/// 自作のinput関数を纏めたクラス。必ずインスタンスすること
/// </summary>
public class MyInput : MonoBehaviour {

    private float beforeTrigger = 0;
    /// <summary>
    /// 軸入力が行われた瞬間のみtrueを返す
    /// </summary>
    /// <param name="_button"></param>
    /// <returns></returns>
    public bool GetButtonDown(string _button) {


        if (Mathf.Abs(Input.GetAxisRaw(_button)) == 1 && beforeTrigger == 0) {
            beforeTrigger = Input.GetAxisRaw(_button);
            return true;
        }
        
        beforeTrigger = Input.GetAxisRaw(_button);
        return false;
    }

    /// <summary>
    /// 軸入力がある間trueを返す
    /// </summary>
    /// <param name="_button"></param>
    /// <returns></returns>
    public bool GetButton(string _button) {
        if (Mathf.Abs(Input.GetAxisRaw(_button)) == 1) {
            return true;
        }
        return false;
    }
}