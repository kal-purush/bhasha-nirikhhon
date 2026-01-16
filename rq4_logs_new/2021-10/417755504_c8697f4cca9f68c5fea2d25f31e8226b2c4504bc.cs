using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>　入力抽象クラス </summary>
public class BasePlayerController : MonoBehaviour
{
    /// <summary>前進するか</summary>
    internal bool m_gasPedal = false;
    /// <summary>右回点するか</summary>
    internal bool m_rightRotaion = false;
    /// <summary>左回転するか</summary>
    internal bool m_leftRotaion = false;
    /// <summary>後進するか</summary>
    internal bool m_BackPedal = false;
    /// <summary>ジャンプするか</summary>
    internal bool m_Jump = false;

    public bool GetGasPedal() => m_gasPedal;
    public bool GetRightRotation() => m_rightRotaion;
    public bool GetLeftRotation() => m_leftRotaion;
    public bool GetBackPedal() => m_BackPedal;
    public bool GetJump() => m_Jump;
}