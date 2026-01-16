using UnityEngine;

public class EnemyController : ControllerBase {
    [Tooltip("Pawn's Line Of Sight range")]
    public float pawnLineOfSight = 5.0f;
    [Tooltip("Pawn's Line Of Sight angle")]
    public float pawnLOSAngle = 45f;

    [Tooltip("Pawn's Hearing radius")]
    public float pawnHearingRadius = 2.5f;

    [Tooltip("Pawn's distance from destination")]
    public float pawnStoppingDistance = 0.5f;

    [Tooltip("Pawn's distance sprinting threshold")]
    public float pawnSprintThreshold = 10.0f;

    [Tooltip("Pawn's AI decision timming")]
    public float pawnBrainTick = 3.0f;

    [Range(0, 1)][Tooltip("Pawn's jumping frequency percentage")]
    public float pawnJumpingFrequency = 1f;

    private bool playerNearby = false;
    private float pawnBrainNextInterval;

    private float pawnDistance;

    // Use this for initialization
    override protected void Start () {
        base.Start();

        pawnBrainNextInterval = pawnBrainTick;

        //Initial actions setup
        pawnExecuteActions();
    }
	
	// Update is called once per frame
	override protected void Update () {
        inputHorizontal = 0.0f;
        inputVertical = 0.0f;
        inputSprint = false;
        inputJump = false;

        base.Update();

        //Controller character timed actions
        if (Time.time >= pawnBrainNextInterval)
        {
            pawnExecuteActions();

            pawnBrainNextInterval += pawnBrainTick;
        }
	}

    // Update is called once per frame
    override protected void FixedUpdate()
    {
        pawnDistance = Vector3.Distance(globalGameController.playerRef.transform.position, transform.position);
        
        //Check if player should move
        if (playerNearby && pawnDistance > pawnStoppingDistance)
        {
            Vector3 pawnNormalizedDifference = Vector3.Normalize(globalGameController.playerRef.transform.position - transform.position);
            inputHorizontal = pawnNormalizedDifference.z;
            inputVertical = -1 * pawnNormalizedDifference.x;

            //Rotate the pawn to face the player
            transform.LookAt(globalGameController.playerRef.transform.position, Vector3.up);
        }

        //Check if should sprint
        inputSprint = (pawnDistance > pawnSprintThreshold);

        //Check if player is still within range
        playerNearby = CheckPlayerDetected();

        base.FixedUpdate();
    }

    private void pawnExecuteActions()
    {
        //Jump randomly
        if (isGrounded)
            inputJump = (Random.Range(0.0f, 1.0f) <= pawnJumpingFrequency);
    }

    private bool CheckPlayerDetected()
    {

        //Check if can hear the player
        if (globalGameController.playerUnit.isMoving && pawnDistance < pawnHearingRadius)
        {
            return true;
        }

        //Check if can see the player
        if (pawnDistance < pawnLineOfSight)
        {
            //Check if is in the cone of sight
            if (Vector3.Angle(transform.forward, (globalGameController.playerRef.transform.position - transform.position)) < pawnLOSAngle) {
                return true;
            }
        }

        return false;
    }
}