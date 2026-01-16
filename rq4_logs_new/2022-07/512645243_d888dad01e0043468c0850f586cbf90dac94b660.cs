using UnityEngine;
using System;
using System.Collections;


public class IKControl : MonoBehaviour
{

    public Animator animator;

    public bool ikActive = false;
    public Transform lookObj = null;

    void Start()
    {
    }

    //a callback for calculating IK
    void OnAnimatorIK()
    {
        if (animator)
        {

            //if the IK is active, set the position and rotation directly to the goal. 
            if (ikActive)
            {

                // Set the look target position, if one has been assigned
                if (lookObj != null)
                {
                    animator.SetLookAtWeight(1);
                    animator.SetLookAtPosition(lookObj.position);

                   // animator.SetIKPositionWeight(AvatarIKGoal.RightHand, 1);
                    animator.SetIKRotationWeight(AvatarIKGoal.RightHand, 1);
                    animator.SetIKPositionWeight(AvatarIKGoal.LeftHand, 1);
                    animator.SetIKPositionWeight(AvatarIKGoal.RightHand, 1);

                    //animator.SetIKPosition(AvatarIKGoal.RightHand, rightHandObj.position);
                    animator.SetIKRotation(AvatarIKGoal.RightHand, lookObj.rotation);
                    animator.SetIKRotation(AvatarIKGoal.LeftHand, lookObj.rotation);
                    animator.SetIKPosition(AvatarIKGoal.RightHand, lookObj.position);
                    animator.SetIKPosition(AvatarIKGoal.LeftHand, lookObj.position);
                }
            }

            //if the IK is not active, set the position and rotation of the hand and head back to the original position
            else
            {
                //animator.SetIKPositionWeight(AvatarIKGoal.RightHand, 0);
                animator.SetIKRotationWeight(AvatarIKGoal.RightHand, 0);
                animator.SetLookAtWeight(0);
            }
        }
    }
}