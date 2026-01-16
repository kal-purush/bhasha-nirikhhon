using UnityEngine;
using System.Collections;

public class EnemyState_Attack : State<Enemy>
{
    private GameObject _target;

    public override void Enter(Enemy owner)
    {
        if (true)
        {
            Debug.LogError("Monster의 선택된 스킬이 없습니다.");
            owner.ChangeState(CHARACTER_STATE.IDLE);
            return;
        }

        //owner.SetAnimation(owner.CurSkill.Anim);
        owner.MoveStop( );
        _target = owner.TargetObj;

        owner.StartCoroutine(this.ProcessSkill(owner));
    }

    public override void Update(Enemy owner)
    {
        owner.RotateToTarget( );
    }


    public override void Exit(Enemy owner)
    {
    }

    private IEnumerator ProcessSkill(Enemy owner)
    {
        
        // 시작시간 대기
        //yield return new WaitForSeconds(skill.StartDelay);

        //owner.SetAnimation(owner.CurSkill.Anim);
        //skill.ProcessSkill<Enemy>(owner, _target);
        Debug.Log("Attack!");

        //yield return new WaitForSeconds(skill.LateDelay);

        if (!owner.IsAlive)
            yield break;

        owner.ChangeState(CHARACTER_STATE.IDLE);
    }
}