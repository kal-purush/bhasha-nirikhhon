using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Game.Characters.Animations
{
    [CreatableAsset][CreateAssetMenu(fileName = "ActionAnimation", menuName = "Animations/ActionAnimation")]
    public class ActionAnimation : ScriptableObject
    {
        [SerializeField]
        AnimationClip _Clip;

        public void AddToAnimation(Animation animation)
        {
            animation.AddClip(_Clip, name);
        }

        public void Play(Animation animation)
        {
            animation.Play(name);
        }

        public void RemoveFromAnimation(Animation animation)
        {
            animation.RemoveClip(_Clip);
        }
    }
}