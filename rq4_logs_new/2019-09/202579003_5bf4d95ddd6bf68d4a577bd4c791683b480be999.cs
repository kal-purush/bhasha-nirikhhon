using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyCharge : Enemy1
{

   



    public string groundMask = "Ground";

    [Header("Wait")]

    [SerializeField]
    float Wait_Cooldown;
    [SerializeField]
    float Wait_Monitoring;

    [Header("State")]

    [SerializeField]
    JamSpace.EState currentEState;
    [SerializeField]
    Animator myAnimator;
    [SerializeField]
    Collider2D boxCollider2d;

    // ability to sense presence

    [SerializeField]
    private bool isMovingRight;
    private const float rotationEulerAngle = 180;


    private void EvaluateRotation()
    {
        if (isMovingRight)
        {
            transform.eulerAngles = new Vector3(0, 0, 0);
        }
        else
        {
            transform.eulerAngles = new Vector3(0, -180, 0);
        }


    }

    void Start()
    {
        targetPoint = point2.position;
        EvaluateRotation();
        //transform.eulerAngles = new Vector3(0, -rotationEulerAngle, 0);

        //int intLMask = LayerMask.GetMask(groundMask);
        myAnimator = this.GetComponent<Animator>();
    }
    void TriggerAnimation(string animTag)
    {
        return;
        myAnimator.SetTrigger(animTag);
    }
    // Call on enemy activation event
    public override void Init()
    {
        CurrentState = JamSpace.EState.IDLE;
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

    // experimental
    [SerializeField]
    float moveSpeed;

    [SerializeField]
    bool moveRight;
    [SerializeField]
    float chargeSpeed;
    [SerializeField]
    float normalSpeed;
    public float rayCastDistance;
    public Common.HealthSetter InvestigatedTargetHealthSetter = null;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        var collisionScript = collision.GetComponent<Player.Movement.PlayerCollision>();
        if (collisionScript)
        {
            InvestigatedTargetHealthSetter = collisionScript.GetComponent<Common.HealthSetter>();
            CurrentState = JamSpace.EState.MONITORING;
        }
    }
    private void OnTriggerExit2D(Collider2D collision)
    {
        var collisionScript = collision.GetComponent<Player.Movement.PlayerCollision>();
        if (collisionScript)
        {
            InvestigatedTargetHealthSetter = null;
        }
    }
    void ExecuteState(JamSpace.EState _enemyState)
    {
        switch (_enemyState)
        {
            case JamSpace.EState.NONE:
                break;
            case JamSpace.EState.IDLE:
                moveSpeed = normalSpeed;
                TriggerAnimation(JamSpace.AnimationTags.ANIMATION_IDLE);
                break;
            case JamSpace.EState.MONITORING:
                StartCoroutine(CallMonitoring());
                break;
            case JamSpace.EState.ATTACKING:
                Attack();
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
    void Attack()
    {
        moveSpeed = chargeSpeed;
        TriggerAnimation(JamSpace.AnimationTags.ANIMATION_CHARGE);
    }
    IEnumerator CallCooldown()
    {
        moveSpeed = 0;
        TriggerAnimation(JamSpace.AnimationTags.ANIMATION_COOLDOWN);
        yield return new WaitForSeconds(Wait_Cooldown);
        CurrentState = JamSpace.EState.IDLE;
    }
    IEnumerator CallMonitoring()
    {
        moveSpeed = 0;
        TriggerAnimation(JamSpace.AnimationTags.ANIMATION_INVESTIGATE);
        yield return new WaitForSeconds(Wait_Monitoring);


        if (InvestigatedTargetHealthSetter)
        {
            if (!isEnemy1)
            {
                targetPoint = InvestigatedTargetHealthSetter.gameObject.transform.position;
            }

            CurrentState = JamSpace.EState.ATTACKING;
        }
        else
        {
            CurrentState = JamSpace.EState.IDLE;
        }
    }
    void Update()
    {
        Move();
    }

    private Vector2 GetRandomPosititonBetweenPoints(ref Transform point1, ref Transform point2)
    {
        float randomXValue = 0f;
        randomXValue = Random.Range(point1.position.x, point2.position.x);
        return new Vector2(randomXValue, point2.position.y);
    }

    private void Move()
    {
        transform.position =
            Vector2.MoveTowards(transform.position, targetPoint, moveSpeed * Time.deltaTime);

        if (Mathf.Approximately(transform.position.x, targetPoint.x) &&
            Mathf.Approximately(transform.position.y, targetPoint.y))
        {
            GetNextPoint();
        }
    }

    private void GetNextPoint()
    {
        if (Mathf.Approximately(point1.position.x, targetPoint.x) &&
            Mathf.Approximately(point1.position.y, targetPoint.y))
        {
            targetPoint = point2.transform.position;
            isMovingRight = !isMovingRight;
            EvaluateRotation();
        }
        else if (Mathf.Approximately(point2.position.x, targetPoint.x) &&
            Mathf.Approximately(point2.position.y, targetPoint.y))
        {
            targetPoint = point1.transform.position;
            isMovingRight = !isMovingRight;
            EvaluateRotation();
        }
        else
        {
            targetPoint = GetRandomPosititonBetweenPoints(ref point1, ref point2);
        }

        moveSpeed = normalSpeed;
        if (currentEState == JamSpace.EState.ATTACKING)
        {
            CurrentState = JamSpace.EState.COOLDOWN;
        }
    }

    [SerializeField]
    private Transform point1;
    // transform pt2 on right
    [SerializeField]
    private Transform point2;
    [SerializeField]
    public Vector2 targetPoint;

}