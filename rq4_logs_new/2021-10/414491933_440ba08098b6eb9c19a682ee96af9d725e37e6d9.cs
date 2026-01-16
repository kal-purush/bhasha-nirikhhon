using System.Linq;
using UnityEngine;

public static class ExtensionMethods
{
    public static float GetAnimationLength(this Animator anim, string animName)
    { // Return the first animation clips length where its name is equal to animName
        return anim.runtimeAnimatorController.animationClips.First(x => x.name == animName).length;
    }
}