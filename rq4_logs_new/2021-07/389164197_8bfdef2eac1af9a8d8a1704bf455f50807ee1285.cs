using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;
public class EnemyBehavior : MonoBehaviour
{
    public GameObject player;
    public float FOVradius;
    [Range(0, 360)]
    public float FOVangle;

    [SerializeField] private LayerMask targetMask;
    [SerializeField] private LayerMask obstructionMask;
    UnityStandardAssets.Characters.ThirdPerson.ThirdPersonCharacter animation;
    public bool playerInView;

    NavMeshAgent agent;
    // Start is called before the first frame update
    void Start()
    {
        agent = this.GetComponent<NavMeshAgent>();
        StartCoroutine(FOVRoutine());
        animation = gameObject.GetComponent<UnityStandardAssets.Characters.ThirdPerson.ThirdPersonCharacter>();
    }

    // Update is called once per frame
    void Update()
    {
        checkPlayerCaught();
    }

    private IEnumerator FOVRoutine()
    {
        WaitForSeconds wait = new WaitForSeconds(0.2f);
        while (true)
        {
            yield return wait;
            FieldOfViewCheck();
        }
    }

    private void FieldOfViewCheck()
    {
        Collider[] rangeChecks = Physics.OverlapSphere(this.transform.position, this.FOVradius, this.targetMask);

        if (rangeChecks.Length != 0)
        {
            Transform target = rangeChecks[0].transform;
            Vector3 directionToTarget = (target.position - transform.position);

            if (Vector3.Angle(transform.forward, directionToTarget) < FOVangle / 2)
            {
                float distanceToTarget = Vector3.Distance(transform.position, target.position);

                if (!Physics.Raycast(transform.position, directionToTarget, distanceToTarget, obstructionMask))
                {
                    playerInView = true;
                    agent.SetDestination(player.transform.position);

                    Vector3 forward = (player.transform.position - gameObject.transform.position).normalized;
                    animation.Move(forward*0.5f, false, false);
                    
                }
                else
                {
                    playerInView = false;
                    agent.ResetPath();
                    animation.Move(Vector3.zero, false, false);
                }
            }

            else playerInView = false;
        }

        else if (playerInView == true)
        {
            playerInView = false;
        }


    }

    private void checkPlayerCaught()
    {
        if (Vector3.Distance(gameObject.transform.position, player.transform.position) < 0.8f)
        {
            //Debug.Log("You have been caught");
            EventBroadcaster.Instance.PostEvent(EventNames.ON_PLAYER_CAUGHT);
        }
    }
}
