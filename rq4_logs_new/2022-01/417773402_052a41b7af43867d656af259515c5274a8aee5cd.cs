using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Game.Characters.Animations
{
    [RequireComponent(typeof(Animation))]
    public class AnimationController : MonoBehaviour
    {
        System.Action[] _CurrentAnimationEvents;

        Animation _Animation;

        Dictionary<string, AnimationClipAndEvents> _AnimationClipAndEventsDictionary = new Dictionary<string, AnimationClipAndEvents>();

        struct AnimationClipAndEvents
        {
            public AnimationClip animationClip;
            public System.Action[] animationEvents;

            public AnimationClipAndEvents(AnimationClip animationClip, System.Action[] animationEvents)
            {
                this.animationClip = animationClip;
                this.animationEvents = animationEvents;
            }
        }

        void Awake() 
        {
            _Animation = GetComponent<Animation>();
        }

        public void AddAnimation(string name, AnimationClip animationClip, params System.Action[] animationEvents)
        {
            _AnimationClipAndEventsDictionary.Add(name, new AnimationClipAndEvents(animationClip, animationEvents));

            _Animation.AddClip(animationClip, name);
        }

        public void RemoveAnimation(string name)
        {
            if(!_AnimationClipAndEventsDictionary.ContainsKey(name))
                return;

            AnimationClip animationClip = _AnimationClipAndEventsDictionary[name].animationClip;

            _Animation.RemoveClip(animationClip);

            _AnimationClipAndEventsDictionary.Remove(name);
        }

        public void Play(string name)
        {
            if(!_AnimationClipAndEventsDictionary.ContainsKey(name))
                return;

            _CurrentAnimationEvents = _AnimationClipAndEventsDictionary[name].animationEvents;

            _Animation.Play(name);
        }

        public void Stop()
        {
            _Animation.Stop();
        }

        public void Stop(string name)
        {
            if(!_AnimationClipAndEventsDictionary.ContainsKey(name))
                return;

            _Animation.Stop(name);
        }

        public void CrossFadePlay(string name, float fadeTime)
        {
            _Animation.CrossFade(name, fadeTime);
        }

        public void InvokeEvent(int index)
        {
            if(index > _CurrentAnimationEvents.Length || index < 0)
            {
                Debug.LogError("Invoke Event index is out of bounds!");

                return;
            }

            _CurrentAnimationEvents[index]?.Invoke();
        }
    }
}