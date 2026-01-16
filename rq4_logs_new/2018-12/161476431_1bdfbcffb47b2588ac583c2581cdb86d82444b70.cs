using UnityEngine;
using UnityEngine.Events;

public class CharacterController2D : MonoBehaviour
{

    [Header("Movimiento")]
    [Space]

    [SerializeField] private float m_RunSpeed = 40f;
    [SerializeField] private float m_JumpForce = 450f;                          // Amount of force added when the player jumps.
    [Range(0, 1)] [SerializeField] private float m_CrouchSpeed = .4f;          // Amount of maxSpeed applied to crouching movement. 1 = 100%
    [Range(0, .3f)] [SerializeField] private float m_MovementSmoothing = .05f;  // How much to smooth out the movement
    [SerializeField] private bool m_AirControl = false;                         // Whether or not a player can steer while jumping;
    [SerializeField] private LayerMask m_WhatIsGround;                          // A mask determining what is ground to the character
    [SerializeField] private Transform m_GroundCheck;                           // A position marking where to check if the player is grounded.
    [SerializeField] private Transform m_CeilingCheck;                          // A position marking where to check for ceilings
    [SerializeField] private Collider2D m_CrouchDisableCollider;                // A collider that will be disabled when crouching

    const float k_GroundedRadius = .2f; // Radius of the overlap circle to determine if grounded
    private bool m_Grounded;            // Whether or not the player is grounded.
    const float k_CeilingRadius = .2f; // Radius of the overlap circle to determine if the player can stand up
    private Rigidbody2D m_Rigidbody2D;
    private SpriteRenderer m_SpriteRenderer;
    private bool m_FacingRight = true;  // For determining which way the player is currently facing.
    private Vector3 m_Velocity = Vector3.zero;

    [Header("Ataque")]
    [Space]

    [SerializeField] private GameObject m_Boomerang;

    [Range(1, 10)] [SerializeField] private float m_AtkReach = 2f;
    [Range(.5f, 5)] [SerializeField] private float m_AtkDuration = 1f;
    [SerializeField] private bool m_AtkActiveInReturn = false;

    [SerializeField] private AnimationCurve m_AtkForwardAnimCurve;
    [SerializeField] private AnimationCurve m_AtkBackwardAnimCurve;


    private CircleCollider2D m_BoomerangCollider;
    private SpriteRenderer m_BoomerangSprite;

    private float m_AtkThrowTimestamp;
    private float m_AtkExtensionTimestamp;
    private float m_AtkRetrieveTimestamp;

    private Vector3 m_AtkStartPos;
    private Vector3 m_AtkTargetPos;

    private bool m_IsAttacking = false;


    [Header("Eventos")]
    [Space]

    public UnityEvent OnLandEvent;

    [System.Serializable]
    public class BoolEvent : UnityEvent<bool> { }

    public BoolEvent OnCrouchEvent;
    public BoolEvent OnBoomerangRetrievedEvent;
    private bool m_wasCrouching = false;

    private void Awake()
    {
        m_Rigidbody2D = GetComponent<Rigidbody2D>();
        m_SpriteRenderer = GetComponent<SpriteRenderer>();
        m_BoomerangCollider = m_Boomerang.GetComponent<CircleCollider2D>();
        m_BoomerangSprite = m_Boomerang.GetComponent<SpriteRenderer>();
        m_BoomerangSprite.enabled = false;
        m_BoomerangCollider.enabled = false;
        m_Boomerang.transform.position = transform.position;

        if (OnLandEvent == null)
            OnLandEvent = new UnityEvent();

        if (OnBoomerangRetrievedEvent == null)
            OnBoomerangRetrievedEvent = new BoolEvent();

        if (OnCrouchEvent == null)
            OnCrouchEvent = new BoolEvent();
    }

    private void Update() {
        if (m_IsAttacking) {
            if (Time.time > m_AtkRetrieveTimestamp || Time.time < m_AtkThrowTimestamp) {
                m_IsAttacking = false;
              
                if (m_BoomerangCollider.isActiveAndEnabled || m_BoomerangSprite.isVisible) {
                    OnBoomerangRetrievedEvent.Invoke(true);
                    m_BoomerangSprite.enabled = false;
                    m_BoomerangCollider.enabled = false;
                }

                m_Boomerang.transform.localPosition = Vector3.zero;
            }
            else if (Time.time >= m_AtkExtensionTimestamp) {
                if (m_BoomerangCollider.isActiveAndEnabled && !m_AtkActiveInReturn) {
                    m_BoomerangCollider.enabled = false;
                }
                m_Boomerang.transform.position = Vector3.Lerp(m_AtkTargetPos, transform.position, m_AtkBackwardAnimCurve.Evaluate((Time.time - m_AtkExtensionTimestamp) / (m_AtkRetrieveTimestamp - m_AtkExtensionTimestamp)));
            }
            else if (Time.time > m_AtkThrowTimestamp && Time.time < m_AtkExtensionTimestamp) {

                m_Boomerang.transform.position = Vector3.Lerp(m_AtkStartPos, m_AtkTargetPos, m_AtkForwardAnimCurve.Evaluate((Time.time - m_AtkThrowTimestamp) / (m_AtkExtensionTimestamp - m_AtkThrowTimestamp)));
            }
            
        }
    }

    private void FixedUpdate()
    {
        bool wasGrounded = m_Grounded;
        m_Grounded = false;

        // The player is grounded if a circlecast to the groundcheck position hits anything designated as ground
        // This can be done using layers instead but Sample Assets will not overwrite your project settings.
        Collider2D[] colliders = Physics2D.OverlapCircleAll(m_GroundCheck.position, k_GroundedRadius, m_WhatIsGround);
        for (int i = 0; i < colliders.Length; i++)
        {
            if (colliders[i].gameObject != gameObject)
            {
                m_Grounded = true;
                if (!wasGrounded)
                    OnLandEvent.Invoke();
            }
        }

        
    }


    public void Move(float move, bool crouch, bool jump)
    {
        move *= m_RunSpeed;
        // If crouching, check to see if the character can stand up
        if (!crouch)
        {
            // If the character has a ceiling preventing them from standing up, keep them crouching
            if (Physics2D.OverlapCircle(m_CeilingCheck.position, k_CeilingRadius, m_WhatIsGround))
            {
                crouch = true;
            }
        }

        //only control the player if grounded or airControl is turned on
        if (m_Grounded || m_AirControl)
        {

            // If crouching
            if (crouch)
            {
                if (!m_wasCrouching)
                {
                    m_wasCrouching = true;
                    OnCrouchEvent.Invoke(true);
                }

                // Reduce the speed by the crouchSpeed multiplier
                move *= m_CrouchSpeed;

                // Disable one of the colliders when crouching
                if (m_CrouchDisableCollider != null)
                    m_CrouchDisableCollider.enabled = false;
            }
            else
            {
                // Enable the collider when not crouching
                if (m_CrouchDisableCollider != null)
                    m_CrouchDisableCollider.enabled = true;

                if (m_wasCrouching)
                {
                    m_wasCrouching = false;
                    OnCrouchEvent.Invoke(false);
                }
            }

            // Move the character by finding the target velocity
            Vector3 targetVelocity = new Vector2(move * 10f, m_Rigidbody2D.velocity.y);
            // And then smoothing it out and applying it to the character
            m_Rigidbody2D.velocity = Vector3.SmoothDamp(m_Rigidbody2D.velocity, targetVelocity, ref m_Velocity, m_MovementSmoothing);

            // If the input is moving the player right and the player is facing left...
            if (move > 0 && !m_FacingRight)
            {
                // ... flip the player.
                Flip();
            }
            // Otherwise if the input is moving the player left and the player is facing right...
            else if (move < 0 && m_FacingRight)
            {
                // ... flip the player.
                Flip();
            }
        }
        // If the player should jump...
        if (m_Grounded && jump)
        {
            // Add a vertical force to the player.
            m_Grounded = false;
            m_Rigidbody2D.AddForce(new Vector2(0f, m_JumpForce));
        }
    }

    public void Attack() {

        if (!m_IsAttacking)
        {
            float currentTime=Time.time;
            m_AtkThrowTimestamp = currentTime;
            m_AtkExtensionTimestamp = currentTime + m_AtkDuration/2f;
            m_AtkRetrieveTimestamp = currentTime + m_AtkDuration;

            m_AtkStartPos = transform.position;
            m_AtkTargetPos = transform.position + m_AtkReach * ((m_FacingRight)? Vector3.right : Vector3.left);

            m_Boomerang.transform.position = transform.position;
            m_BoomerangCollider.enabled = true;
            m_BoomerangSprite.enabled = true;

            OnBoomerangRetrievedEvent.Invoke(false);
            m_IsAttacking = true;
        }
        
    }

    private void Flip()
    {
        // Switch the way the player is labelled as facing.
        m_FacingRight = !m_FacingRight;

        m_SpriteRenderer.flipX = !m_FacingRight;
        /*
        // Multiply the player's x local scale by -1.
        Vector3 theScale = transform.localScale;
        theScale.x *= -1;
        transform.localScale = theScale;
        */
    }
}