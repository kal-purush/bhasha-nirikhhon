using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Enemy3 : BaseNPC
{
    public Transform point1;
    public Transform point2;
    public Vector2 targetPoint;

    public Vector2 GetRandomPosititonBetweenPoints(ref Transform point1, ref Transform point2)
    {
        float yValue = 0f;
        float randomXValue = 0f;
        randomXValue = Random.Range(point1.position.x, point2.position.x);
        return new Vector2(randomXValue, yValue);
    }

    public float thetaRange;
    public float maxHeight;                                                        

    public string groundMask = "Ground";

    [Header("Wait")]

    [SerializeField]
    float Wait_Cooldown;
    [SerializeField]
    float Wait_Monitoring;

    [Header("State")]

    [SerializeField]
    JamSpace.EState currentEState;

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


    [SerializeField]
    Animator myAnimator;
    [SerializeField]
    BoxCollider2D boxCollider2d;

    // ability to sense presence

    void Start()
    {
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
    // experimental
    [SerializeField]
    float moveSpeed;
    [SerializeField]
    Transform platformEndDetector;
    bool moveRight;
    [SerializeField]
    float chargeSpeed;
    [SerializeField]
    float normalSpeed;
    public float rayCastDistance;
    public Common.HealthSetter InvestigatedTargetHealthSetter = null;

    [SerializeField]
    Text _text;


    private void OnTriggerStay2D(Collider2D collision)
    {

        if (CurrentState == JamSpace.EState.IDLE)
        {
            var collisionScript = collision.GetComponent<Player.Movement.PlayerCollision>();
            if (collisionScript)
            {
                InvestigatedTargetHealthSetter = collisionScript.GetComponent<Common.HealthSetter>();
                Vector2 _vec2 = this.gameObject.transform.position - InvestigatedTargetHealthSetter.transform.position;
                float _angle = Vector2.Angle(Vector2.right, _vec2);

                if (_angle <= thetaRange)
                {
                    //_text.text = "monitoring";
                    CurrentState = JamSpace.EState.MONITORING;
                    //ExecuteState(JamSpace.EState.MONITORING);
                }
                else
                {
                    var _tempAngle = 180 - thetaRange;
                    var _subAngle = _angle - _tempAngle;
                    if (((_subAngle) <= thetaRange) && (_subAngle > 0))
                    {
                        //_text.text = "monitoring";
                        CurrentState = JamSpace.EState.MONITORING;

                    }

                    //_text.text = "not in range";
                }
            }
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {




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
            CurrentState = JamSpace.EState.ATTACKING;
        }
        else
        {
            CurrentState = JamSpace.EState.IDLE;
        }
    }
    void RotateCharacter()
    {
        if (moveRight)
        {
            transform.eulerAngles = new Vector3(0, -180, 0);
            transform.Translate(moveSpeed * Time.deltaTime * Vector2.right);
        }
        else
        {
            transform.eulerAngles = new Vector3(0, 0, 0);
            transform.Translate(moveSpeed * Time.deltaTime * Vector2.right);
        }

        moveRight = !moveRight;
    }
    void ReachedEndPoint()
    {
        RotateCharacter();
        moveSpeed = normalSpeed;

        if (CurrentState == JamSpace.EState.ATTACKING)
        {
            CurrentState = JamSpace.EState.COOLDOWN;
        }
    }

    void Move()
    {
        //transform.position = Vector2.MoveTowards(transform.position,
        //                                        targetPoint,
        //                                        moveSpeed * Time.deltaTime);
        //if (transform.position == waypoints[wayPointIndex].transform.position)
        //{
        //    wayPointIndex += 1;
        //}

        //if (wayPointIndex == waypoints.Length)
        //    wayPointIndex = 0;
    }

    void Update()
    {
        Move();


        //int intLMask = LayerMask.GetMask(groundMask);

        //transform.Translate(Vector2.right * moveSpeed * Time.deltaTime);
        //RaycastHit2D groundInfo = Physics2D.Raycast(platformEndDetector.position, Vector2.down, rayCastDistance, intLMask);
        //Debug.DrawRay(platformEndDetector.position, Vector2.down, Color.red, 3f);
        //if (groundInfo.collider == false)
        //{
        //    ReachedEndPoint();
        //}
    }
}