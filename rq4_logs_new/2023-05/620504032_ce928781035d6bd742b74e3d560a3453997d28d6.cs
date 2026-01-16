using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CaretakerController : MonoBehaviour
{
    [SerializeField] Animator LeftAnimator;
    [SerializeField] Animator RightAnimator;
    [SerializeField] Transform startingPosition;
    [SerializeField] Transform endPosition;
    [SerializeField] GameObject hands;

    private float startingCryTime;
    private float startingDistanceFromChild;

    private Animator playerAnimator;
    private Health playerHealth;

    private float lastDistanceMoved;
    private float lastPlayerDown;

    private void Start()
    {
        hands.SetActive(false);
    }


    private void Update()
    {
        if (playerAnimator == null)
        {
            playerAnimator = transform.GetComponentInParent<Entity>().animator;
        }
        if (playerHealth == null)
        {
            playerHealth = transform.GetComponentInParent<Entity>().GetComponent<Health>();
        }
        if (playerAnimator.GetCurrentAnimatorStateInfo(0).IsName("Flip"))
        {
            hands.SetActive(true);
            transform.position = startingPosition.position;
            startingCryTime = Time.time;
            startingDistanceFromChild = (startingPosition.position - endPosition.position).magnitude;
        }
        else if (playerAnimator.GetCurrentAnimatorStateInfo(0).IsName("Revive"))
        {
            hands.SetActive(false);
        }
        else if (playerAnimator.GetCurrentAnimatorStateInfo(0).IsName("Crying") && playerHealth.health <= 0)
        {
            float distanceMoved = (100 - playerHealth.down) / 5;

            float sectionOfDistance = distanceMoved / startingDistanceFromChild;

            if (playerHealth.down == lastPlayerDown) sectionOfDistance = lastDistanceMoved;

            transform.position = Vector3.Lerp(startingPosition.position, endPosition.position, sectionOfDistance);

            if (playerHealth.down <= 0)
            {
                SetAnimators();
            }

            lastPlayerDown = playerHealth.down;
            lastDistanceMoved = sectionOfDistance;
        }
    }

    private void SetAnimators()
    {
        LeftAnimator.SetTrigger("isDead");
        RightAnimator.SetTrigger("isDead");
    }

    private void Revive()
    {
        Destroy(gameObject);
    }
}