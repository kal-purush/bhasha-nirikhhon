using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AnimationManager : MonoBehaviour
{
    public string currentStateName;

    public Animator animator;
    private Dictionary<string, float> animationSpeeds;

    void Awake()
    {
        animationSpeeds = new Dictionary<string, float>
        {
            { "Walk", 1.0f },
            { "Idle", 1.0f },
            { "PlayerAttack(0)", 0.4f },

        };

        animator = GetComponent<Animator>();
        animator.speed = 1.0f;
    }

    private void Update()
    {
        AnimatorStateInfo stateInfo = animator.GetCurrentAnimatorStateInfo(0); // 0 for the base layer
        currentStateName = GetStateName(stateInfo.shortNameHash);
    }

    public float GetAnimationStateSpeed(string stateName)
    {
        if (animationSpeeds.ContainsKey(stateName))
        {
            return animator.speed * animationSpeeds[stateName];
        }
        else
        {
            Debug.LogWarning("Animation state not found: " + stateName);
            return animator.speed;
        }
    }

    string GetStateName(int hash)
    {
        foreach (AnimationClip clip in animator.runtimeAnimatorController.animationClips)
        {
            if (Animator.StringToHash(clip.name) == hash)
                return clip.name;
        }
        return "Unknown State";
    }
}