using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Unity.FPS.Gameplay;
using Unity.FPS.Game;

public class PlayerCharacterControllerVR : PlayerCharacterController
{


    //public override bool IsDead { get; protected set; }
    //public override bool HasJumpedThisFrame { get; protected set; }
    //public override bool IsGrounded { get; protected set; }


    //void Awake()
    //{
    //    ActorsManager actorsManager = FindObjectOfType<ActorsManager>();
    //    if (actorsManager != null)
    //        actorsManager.SetPlayer(gameObject);
    //}


    //void Start()
    //{
    //    // fetch components on the same gameObject
    //    m_Controller = GetComponent<CharacterController>();
    //    DebugUtility.HandleErrorIfNullGetComponent<CharacterController, PlayerCharacterControllerVR>(m_Controller,
    //        this, gameObject);

    //    m_InputHandler = GetComponent<PlayerInputHandlerVR>();
    //    DebugUtility.HandleErrorIfNullGetComponent<PlayerInputHandlerVR, PlayerCharacterControllerVR>(m_InputHandler,
    //        this, gameObject);

    //    m_WeaponsManager = GetComponent<PlayerWeaponsManagerVR>();
    //    DebugUtility.HandleErrorIfNullGetComponent<PlayerWeaponsManagerVR, PlayerCharacterControllerVR>(
    //        m_WeaponsManager, this, gameObject);

    //    m_Health = GetComponent<Health>();
    //    DebugUtility.HandleErrorIfNullGetComponent<Health, PlayerCharacterControllerVR>(m_Health, this, gameObject);

    //    m_Actor = GetComponent<Actor>();
    //    DebugUtility.HandleErrorIfNullGetComponent<Actor, PlayerCharacterControllerVR>(m_Actor, this, gameObject);

    //    m_Controller.enableOverlapRecovery = true;

    //    m_Health.OnDie += OnDie;

    //    // force the crouch state to false when starting
    //    //SetCrouchingState(false, true);
    //    UpdateCharacterHeight(true);
    //}

    //void OnDie()
    //{
    //    IsDead = true;

    //    // Tell the weapons manager to switch to a non-existing weapon in order to lower the weapon
    //    m_WeaponsManager.SwitchToWeaponIndex(-1, true);

    //    EventManager.Broadcast(Events.PlayerDeathEvent);
    //}


    //void Update()
    //{
    //    // check for Y kill
    //    if (!IsDead && transform.position.y < KillHeight)
    //    {
    //        m_Health.Kill();
    //    }

    //    HasJumpedThisFrame = false;

    //    bool wasGrounded = IsGrounded;
    //    GroundCheck();

    //    // landing
    //    if (IsGrounded && !wasGrounded)
    //    {
    //        // Fall damage
    //        float fallSpeed = -Mathf.Min(CharacterVelocity.y, m_LatestImpactSpeed.y);
    //        float fallSpeedRatio = (fallSpeed - MinSpeedForFallDamage) /
    //                               (MaxSpeedForFallDamage - MinSpeedForFallDamage);
    //        if (RecievesFallDamage && fallSpeedRatio > 0f)
    //        {
    //            float dmgFromFall = Mathf.Lerp(FallDamageAtMinSpeed, FallDamageAtMaxSpeed, fallSpeedRatio);
    //            m_Health.TakeDamage(dmgFromFall, null);

    //            // fall damage SFX
    //            AudioSource.PlayOneShot(FallDamageSfx);
    //        }
    //        else
    //        {
    //            // land SFX
    //            AudioSource.PlayOneShot(LandSfx);
    //        }
    //    }

    //    // crouching
    //    //if (m_InputHandler.GetCrouchInputDown())
    //    //{
    //    //    SetCrouchingState(!IsCrouching, false);
    //    //}

    //    UpdateCharacterHeight(false);

    //    HandleCharacterMovement();
    //}



    //protected override void HandleCharacterMovement()
    //{
    //    // horizontal character rotation
    //    {
    //        // rotate the transform with the input speed around its local Y axis
    //        transform.Rotate(
    //            new Vector3(0f, (m_InputHandler.GetLookInputsHorizontal() * RotationSpeed * RotationMultiplier),
    //                0f), Space.Self);
    //    }

    //    // vertical camera rotation
    //    {
    //        // add vertical inputs to the camera's vertical angle
    //        m_CameraVerticalAngle += m_InputHandler.GetLookInputsVertical() * RotationSpeed * RotationMultiplier;

    //        // limit the camera's vertical angle to min/max
    //        m_CameraVerticalAngle = Mathf.Clamp(m_CameraVerticalAngle, -89f, 89f);

    //        // apply the vertical angle as a local rotation to the camera transform along its right axis (makes it pivot up and down)
    //        PlayerCamera.transform.localEulerAngles = new Vector3(m_CameraVerticalAngle, 0, 0);
    //    }

    //    // character movement handling
    //    bool isSprinting = m_InputHandler.GetSprintInputHeld();
    //    {
    //        if (isSprinting)
    //        {
    //            isSprinting = SetCrouchingState(false, false);
    //        }

    //        float speedModifier = isSprinting ? SprintSpeedModifier : 1f;

    //        // converts move input to a worldspace vector based on our character's transform orientation
    //        Vector3 worldspaceMoveInput = transform.TransformVector(m_InputHandler.GetMoveInput());


    //        IsGrounded = true; // IsGrounded is always false in VR
    //        // handle grounded movement
    //        if (IsGrounded)
    //        {
    //            // calculate the desired velocity from inputs, max speed, and current slope
    //            Vector3 targetVelocity = worldspaceMoveInput * MaxSpeedOnGround * speedModifier;
    //            // reduce speed if crouching by crouch speed ratio
    //            if (IsCrouching)
    //                targetVelocity *= MaxSpeedCrouchedRatio;
    //            targetVelocity = GetDirectionReorientedOnSlope(targetVelocity.normalized, m_GroundNormal) *
    //                             targetVelocity.magnitude;

    //            // smoothly interpolate between our current velocity and the target velocity based on acceleration speed
    //            CharacterVelocity = Vector3.Lerp(CharacterVelocity, targetVelocity,
    //                MovementSharpnessOnGround * Time.deltaTime);

    //            Debug.Log("IsGrounded " + IsGrounded + " " + worldspaceMoveInput);


    //            // jumping
    //            if (IsGrounded && m_InputHandler.GetJumpInputDown())
    //            {
    //                // force the crouch state to false
    //                if (SetCrouchingState(false, false))
    //                {
    //                    // start by canceling out the vertical component of our velocity
    //                    CharacterVelocity = new Vector3(CharacterVelocity.x, 0f, CharacterVelocity.z);

    //                    // then, add the jumpSpeed value upwards
    //                    CharacterVelocity += Vector3.up * JumpForce;

    //                    // play sound
    //                    AudioSource.PlayOneShot(JumpSfx);

    //                    // remember last time we jumped because we need to prevent snapping to ground for a short time
    //                    m_LastTimeJumped = Time.time;
    //                    HasJumpedThisFrame = true;

    //                    // Force grounding to false
    //                    IsGrounded = false;
    //                    m_GroundNormal = Vector3.up;
    //                }
    //            }

    //            // footsteps sound
    //            float chosenFootstepSfxFrequency =
    //                (isSprinting ? FootstepSfxFrequencyWhileSprinting : FootstepSfxFrequency);
    //            if (m_FootstepDistanceCounter >= 1f / chosenFootstepSfxFrequency)
    //            {
    //                m_FootstepDistanceCounter = 0f;
    //                AudioSource.PlayOneShot(FootstepSfx);
    //            }

    //            // keep track of distance traveled for footsteps sound
    //            m_FootstepDistanceCounter += CharacterVelocity.magnitude * Time.deltaTime;
    //        }
    //        // handle air movement
    //        else
    //        {
    //            // add air acceleration
    //            CharacterVelocity += worldspaceMoveInput * AccelerationSpeedInAir * Time.deltaTime;

    //            // limit air speed to a maximum, but only horizontally
    //            float verticalVelocity = CharacterVelocity.y;
    //            Vector3 horizontalVelocity = Vector3.ProjectOnPlane(CharacterVelocity, Vector3.up);
    //            horizontalVelocity = Vector3.ClampMagnitude(horizontalVelocity, MaxSpeedInAir * speedModifier);
    //            CharacterVelocity = horizontalVelocity + (Vector3.up * verticalVelocity);

    //            // apply the gravity to the velocity
    //            CharacterVelocity += Vector3.down * GravityDownForce * Time.deltaTime;
    //        }
    //    }

    //    // apply the final calculated velocity value as a character movement
    //    Vector3 capsuleBottomBeforeMove = GetCapsuleBottomHemisphere();
    //    Vector3 capsuleTopBeforeMove = GetCapsuleTopHemisphere(m_Controller.height);

    //    //Debug.Log("CharacterVelocity : " + CharacterVelocity);
    //    m_Controller.Move(CharacterVelocity * Time.deltaTime);

    //    // detect obstructions to adjust velocity accordingly
    //    m_LatestImpactSpeed = Vector3.zero;
    //    if (Physics.CapsuleCast(capsuleBottomBeforeMove, capsuleTopBeforeMove, m_Controller.radius,
    //        CharacterVelocity.normalized, out RaycastHit hit, CharacterVelocity.magnitude * Time.deltaTime, -1,
    //        QueryTriggerInteraction.Ignore))
    //    {
    //        // We remember the last impact speed because the fall damage logic might need it
    //        m_LatestImpactSpeed = CharacterVelocity;

    //        CharacterVelocity = Vector3.ProjectOnPlane(CharacterVelocity, hit.normal);
    //    }
    //}



    //void UpdateCharacterHeight(bool force)
    //{
    //    // Update height instantly
    //    if (force)
    //    {
    //        m_Controller.height = m_TargetCharacterHeight;
    //        m_Controller.center = Vector3.up * m_Controller.height * 0.5f;
    //        PlayerCamera.transform.localPosition = Vector3.up * m_TargetCharacterHeight * CameraHeightRatio;
    //        m_Actor.AimPoint.transform.localPosition = m_Controller.center;
    //    }
    //    // Update smooth height
    //    else if (m_Controller.height != m_TargetCharacterHeight)
    //    {
    //        // resize the capsule and adjust camera position
    //        m_Controller.height = Mathf.Lerp(m_Controller.height, m_TargetCharacterHeight,
    //            CrouchingSharpness * Time.deltaTime);
    //        m_Controller.center = Vector3.up * m_Controller.height * 0.5f;
    //        PlayerCamera.transform.localPosition = Vector3.Lerp(PlayerCamera.transform.localPosition,
    //            Vector3.up * m_TargetCharacterHeight * CameraHeightRatio, CrouchingSharpness * Time.deltaTime);
    //        m_Actor.AimPoint.transform.localPosition = m_Controller.center;
    //    }
    //}


    //// Returns true if the slope angle represented by the given normal is under the slope angle limit of the character controller
    //bool IsNormalUnderSlopeLimit(Vector3 normal)
    //{
    //    return Vector3.Angle(transform.up, normal) <= m_Controller.slopeLimit;
    //}


    //// Gets the center point of the bottom hemisphere of the character controller capsule    
    //Vector3 GetCapsuleBottomHemisphere()
    //{
    //    return transform.position + (transform.up * m_Controller.radius);
    //}

    //// Gets the center point of the top hemisphere of the character controller capsule    
    //Vector3 GetCapsuleTopHemisphere(float atHeight)
    //{
    //    return transform.position + (transform.up * (atHeight - m_Controller.radius));
    //}

    //void GroundCheck()
    //{
    //    // Make sure that the ground check distance while already in air is very small, to prevent suddenly snapping to ground
    //    float chosenGroundCheckDistance =
    //        IsGrounded ? (m_Controller.skinWidth + GroundCheckDistance) : k_GroundCheckDistanceInAir;

    //    // reset values before the ground check
    //    IsGrounded = false;
    //    m_GroundNormal = Vector3.up;

    //    // only try to detect ground if it's been a short amount of time since last jump; otherwise we may snap to the ground instantly after we try jumping
    //    if (Time.time >= m_LastTimeJumped + k_JumpGroundingPreventionTime)
    //    {
    //        // if we're grounded, collect info about the ground normal with a downward capsule cast representing our character capsule
    //        if (Physics.CapsuleCast(GetCapsuleBottomHemisphere(), GetCapsuleTopHemisphere(m_Controller.height),
    //            m_Controller.radius, Vector3.down, out RaycastHit hit, chosenGroundCheckDistance, GroundCheckLayers,
    //            QueryTriggerInteraction.Ignore))
    //        {
    //            // storing the upward direction for the surface found
    //            m_GroundNormal = hit.normal;

    //            // Only consider this a valid ground hit if the ground normal goes in the same direction as the character up
    //            // and if the slope angle is lower than the character controller's limit
    //            if (Vector3.Dot(hit.normal, transform.up) > 0f &&
    //                IsNormalUnderSlopeLimit(m_GroundNormal))
    //            {
    //                IsGrounded = true;

    //                // handle snapping to the ground
    //                if (hit.distance > m_Controller.skinWidth)
    //                {
    //                    m_Controller.Move(Vector3.down * hit.distance);
    //                }
    //            }
    //        }
    //    }
    //}


}