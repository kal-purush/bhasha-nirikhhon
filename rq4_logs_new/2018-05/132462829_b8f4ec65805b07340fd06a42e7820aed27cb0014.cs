using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/**
 * 攻击流程脚本
 */ 
public class Attack : MonoBehaviour {

    private Vector3 startPos;
    private Vector3 endPos;
    private float speed;

    public bool isGoing;
    public bool isComing;

    // 模型动画机
    private Animator animator;
	// Use this for initialization
	void Start () {
        isGoing = false;
        isComing = false;
        speed = 0.1f;
        animator = this.GetComponent<Animator>();
	}
	
	// Update is called once per frame
	void Update () {
        if (isGoing)
        {
            // 锁住
            God.getInstance().isFree = false;

            transform.LookAt(endPos);
            transform.Translate(Vector3.forward * speed);
            // 动画机切换到run状态
            animator.SetTrigger("run");
            float distance = (endPos - transform.position).sqrMagnitude;
            if (distance < 2)
            {
                isGoing = false;
                // 动画机切换到attack状态
                animator.ResetTrigger("run");
                animator.SetTrigger("attack");
            }
        }
        if (isComing)
        {
            transform.LookAt(startPos);
            transform.Translate(Vector3.forward * speed);
            float distance = (startPos - transform.position).sqrMagnitude;
            if (distance < 0.02)
            {
                isComing = false;
                // 释放
                God.getInstance().isFree = true;
                // hero回到原位置
                transform.position = startPos;
                // 动画机切换到idle状态
                animator.SetTrigger("idle");
            }
        }
	}

    public void go(Vector3 start, Vector3 end)
    {
        this.startPos = start;
        this.endPos = end;
        isGoing = true;
    }

    /**
     * 攻击动画播放完毕时的触发事件
     */ 
    public void attackAnimFinished()
    {
        isComing = true;
        if (null != God.getInstance().currentplayer.SelectedHero2)  // 攻击英雄
        {
            God.getInstance().currentplayer.SelectedHero1.Attack(God.getInstance().currentplayer.SelectedHero2);
        } else // 攻击召唤师
        {
            God.getInstance().currentplayer.SelectedHero1.Attack(God.getInstance().currentplayer.selectedPlayer);
        }
    }
}