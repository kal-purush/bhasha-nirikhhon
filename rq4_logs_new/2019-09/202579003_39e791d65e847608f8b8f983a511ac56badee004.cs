using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class StagnantEnemy : BaseNPC
{
    [SerializeField]
    private GameObject projectilePrefab;

    [SerializeField]
    private float rangeRadius;

    [SerializeField]
    private float playerInsideDur;

    [SerializeField]
    private float readyToAttackDur;

    [SerializeField]
    private Collider2D monitoringRange;

    [SerializeField]
    private float cooldownDur;

    [SerializeField]
    private float readyToCoolDur;

    [SerializeField]
    Animator myAnimator;

    [SerializeField]
    JamSpace.EState currentEState;

    public void TriggerAnimation(string animTag)
    {
        return;
        myAnimator.SetTrigger(animTag);
    }

    public JamSpace.EState CurrentState
    {
        get
        {
            return currentEState;
        }
        set
        {
            currentEState = value;
            ExecuteState(currentEState);
        }
    }

    IEnumerator CallCooldown()
    {
        TriggerAnimation(JamSpace.AnimationTags.ANIMATION_COOLDOWN);
        yield return new WaitForSeconds(cooldownDur);
        CurrentState = JamSpace.EState.IDLE;
    }

    IEnumerator CallMonitoring()
    {
        TriggerAnimation(JamSpace.AnimationTags.ANIMATION_INVESTIGATE);
        playerInsideRangeCounter = readyToAttackDur;
        yield return new WaitForSeconds(readyToAttackDur);

        if (InvestigatedTargetHealthSetter)
        {
            var playerXPos = InvestigatedTargetHealthSetter.transform.position.x;
            var myXPos = this.transform.position.x;

            Projectiles.BaseProjectile baseEnemyProj = null;

            if ((playerXPos - myXPos) < 0)
            {
                // fire in left direction
                baseEnemyProj = Instantiate(projectilePrefab, this.transform.position, Quaternion.Euler(0, -180, 0)).GetComponent<Projectiles.BaseProjectile>();
                baseEnemyProj.GetComponent<Rigidbody2D>().velocity = -speed * baseEnemyProj.transform.right;


            }
            else
            {
                baseEnemyProj = Instantiate(projectilePrefab, this.transform.position, Quaternion.Euler(0, 0, 0)).GetComponent<Projectiles.BaseProjectile>();
                baseEnemyProj.GetComponent<Rigidbody2D>().velocity = speed * baseEnemyProj.transform.right;

                // fire in right direction
            }
        }
        else
        {
            CurrentState = JamSpace.EState.IDLE;
        }
    }

    public Common.HealthSetter InvestigatedTargetHealthSetter = null;


    private void OnTriggerStay2D(Collider2D collision)
    {
        var collisionScript = collision.GetComponent<Player.Movement.PlayerCollision>();
        if (collisionScript)
        {
            InvestigatedTargetHealthSetter = collisionScript.GetComponent<Common.HealthSetter>();

            if (bCanStartCounter)
            {
                CurrentState = JamSpace.EState.MONITORING;
                bCanStartCounter = false;
            }
        }
    }

    private void Update()
    {
    }

    [Header("Counter")]
    [SerializeField]
    private bool bCanStartCounter = false;
    [SerializeField]
    private float playerInsideRangeCounter;

    private void OnTriggerExit2D(Collider2D collision)
    {
        var collisionScript = collision.GetComponent<Player.Movement.PlayerCollision>();
        if (collisionScript)
        {
            InvestigatedTargetHealthSetter = null;
        }
    }

    private void Start()
    {
        this.gameObject.GetComponent<CircleCollider2D>().radius = rangeRadius;
    }

    void ExecuteState(JamSpace.EState _enemyState)
    {
        switch (_enemyState)
        {
            case JamSpace.EState.NONE:
                break;
            case JamSpace.EState.IDLE:
                bCanStartCounter = true;
                TriggerAnimation(JamSpace.AnimationTags.ANIMATION_IDLE);
                break;
            case JamSpace.EState.MONITORING:
                StartCoroutine(CallMonitoring());
                break;
            case JamSpace.EState.ATTACKING:
                StartCoroutine(CallCooldown());
                break;
            case JamSpace.EState.COOLDOWN:
                StartCoroutine(CallCooldown());
                break;
            case JamSpace.EState.MAX:
                break;
            default:
                break;
        }
    }
}