using System.Linq;
using Unity.VisualScripting;
using UnityEngine;

// <summary>
// This class is responsible for controlling animations of a given sprite. 
// </summary>
public class AnimatorBrain : MonoBehaviour, IAnimateSprite
{

    public Animator anim;
    public int currentState;
    public bool lockState = false;
    public int requestedState;

    public int IDLE = Animator.StringToHash("Idle");
    public int MOVE = Animator.StringToHash("Move");
    public int ATTACK = Animator.StringToHash("Attack");
    public int HURT = Animator.StringToHash("Hurt");
    public int DEATH = Animator.StringToHash("Death");
    private void Awake()
    {
        anim = GetComponent<Animator>();
    }

    private void Update()
    {
        CheckLock(anim.GetCurrentAnimatorStateInfo(0));

        if (lockState) return;

        if (requestedState != currentState)
        {
            ChangeAnimationState(requestedState);
        }
    }


    public void ChangeAnimationState(int newState)
    {
        if (currentState == newState) return;

        anim.CrossFade(newState, 0f, 0);

        currentState = newState;
    }

    public void RequestAnimation(int newState)
    {
        if (newState != IDLE && newState != MOVE && newState != ATTACK && newState != HURT && newState != DEATH)
        {
            Debug.LogError("Invalid animation state requested");
            return;
        }

        requestedState = newState;
    }

    public void OnHit()
    {
        lockState = true;
        ChangeAnimationState(HURT);
    }

    public void OnAttack()
    {
        lockState = true;
        ChangeAnimationState(ATTACK);
    }

    public void OnMove()
    {
        RequestAnimation(MOVE);
    }

    public void OnIdle()
    {
        RequestAnimation(IDLE);
    }

    public void OnDeath()
    {
        lockState = true;
        ChangeAnimationState(DEATH);
    }

    public void CheckLock(AnimatorStateInfo stateInfo)
    {
        if (lockState && stateInfo.normalizedTime >= 1 && (stateInfo.IsName("Attack") || stateInfo.IsName("Hurt") || stateInfo.IsName("Death")))
        {
            lockState = false;
        }
        else
        {
            lockState = true;
        }
    }
}