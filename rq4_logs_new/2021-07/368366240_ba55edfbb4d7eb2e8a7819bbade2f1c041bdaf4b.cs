using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// 移動以外の入力アクションをまとめた基底クラス
/// </summary>
public class ActionControlBase : MonoBehaviour
{
    [SerializeField] protected PunchController m_punch = null;
    protected Rigidbody2D m_rb = null;
    protected Animator m_anim = null;
    private void Start()
    {
        m_rb = GetComponent<Rigidbody2D>();
        m_anim = GetComponent<Animator>();
    }
    /// <summary>
    /// ジャンプ
    /// </summary>
    public virtual void Jump(float power)
    {
        m_rb.AddForce(Vector2.up * power, ForceMode2D.Impulse);
    }
    /// <summary>
    /// 弱攻撃
    /// </summary>
    /// <param name="power"></param>
    public virtual void LightAttack(float power)
    {
        m_punch.SetPower(power);
        if (m_anim)
            m_anim.SetBool("Punch",true);
        m_rb.AddForce(Vector2.right * this.transform.localScale.x * power, ForceMode2D.Impulse);
    }
    /// <summary>
    /// 強攻撃
    /// </summary>
    /// <param name="power"></param>
    public virtual void StrongAttack(float power)
    {
        m_punch.SetPower(power);
        if (m_anim)
            m_anim.SetBool("Punch", true);
        m_rb.AddForce(Vector2.right * this.transform.localScale.x * power * power, ForceMode2D.Impulse);
    }
    /// <summary>
    /// 被ダメージ
    /// </summary>
    /// <param name="attackVector"></param>
    public virtual void Hit(Vector3 attackVector)
    {
        m_rb.AddForce(attackVector, ForceMode2D.Impulse);
    }
    public virtual void AttackEnd()
    {
        if (m_anim)
            m_anim.SetBool("Punch", false);
    }
}