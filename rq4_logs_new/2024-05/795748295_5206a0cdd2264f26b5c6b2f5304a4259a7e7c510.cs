using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class CatMovementScript : MonoBehaviour
{
    [Header("Components")]
    [SerializeField] private Animator anim;
    [SerializeField] private SpriteRenderer sr;
    [SerializeField] private AudioSource audioSource;
    [SerializeField] private NavMeshAgent agent;

    [Header("Movement Variables")]
    [SerializeField] private float distanceMax = 1f;
    [SerializeField] private float speed = 0.2f;

    private float sleepTimer = 0f;
    private float sleepTime = 15f;

    private void Start()
    {
        anim = GetComponent<Animator>();
        sr = GetComponent<SpriteRenderer>();
        audioSource = GetComponent<AudioSource>();
        agent = GetComponent<NavMeshAgent>();
        agent.updateRotation = false;
        agent.updateUpAxis = false;
        agent.speed = speed;
        sleepTimer = sleepTime;
    }

    private void Update()
    {
        Flip();
        UpdateAnimation();

        // randomly choose whether to move to a new position
        // or take a nap
        if (sleepTimer >= sleepTime)
        {
            // choose one of two options
            int random = Random.Range(0, 2);
            if (random == 0)
            {
                Debug.Log("Moving to random position");
                anim.SetBool("Sleep", false);
                StartCoroutine(MoveToRandomPosition());
            }
            else
            {
                Debug.Log("Going to sleep");
                StartCoroutine(GoToSleep());
            }
            sleepTimer = 0;
        }
        else
        {
            sleepTimer += Time.deltaTime;
        }

        if (agent.velocity.magnitude < 0.01f)
        {
            anim.SetBool("Moving", false);
        }
    }

    private void UpdateAnimation()
    {
        anim.SetFloat("Speed", agent.velocity.magnitude);
        if (agent.velocity.magnitude > 0)
        {
            anim.SetBool("Moving", true);
            if (agent.velocity.y > 0)
            {
                anim.SetBool("Up", true);
            }
            else
            {
                anim.SetBool("Up", false);
            }

            // if the angle is more than 45 degrees, then it's a side movement
            if (Mathf.Abs(agent.velocity.x) > Mathf.Abs(agent.velocity.y))
            {
                anim.SetBool("Side", true);
            }
            else
            {
                anim.SetBool("Side", false);
            }
        }
        else
        {
            anim.SetBool("Moving", false);
        }
    }

    public void Move(Vector3 destination)
    {
        agent.SetDestination(destination);
    }

    public void Stop()
    {
        agent.velocity = Vector3.zero;
    }

    public void Flip()
    {
        if (agent.velocity.x > 0)
        {
            sr.flipX = true;
        }
        else if (agent.velocity.x < 0)
        {
            sr.flipX = false;
        }
    }

    public void PlaySound(AudioClip clip)
    {
    }

    public void ChooseRandomDestination()
    {
        Vector3 randomDirection = Random.insideUnitSphere * distanceMax;
        randomDirection += transform.position;
        NavMeshHit hit;
        NavMesh.SamplePosition(randomDirection, out hit, distanceMax, 1);
        Vector3 finalPosition = hit.position;
        Move(finalPosition);
    }


    public IEnumerator MoveToRandomPosition()
    {
        ChooseRandomDestination();
        yield return new WaitForSeconds(1f);
    }

    public IEnumerator GoToSleep()
    {
        anim.SetBool("Sleep", true);
        Stop();
        agent.SetDestination(transform.position);
        yield return new WaitForSeconds(5f);
    }
}