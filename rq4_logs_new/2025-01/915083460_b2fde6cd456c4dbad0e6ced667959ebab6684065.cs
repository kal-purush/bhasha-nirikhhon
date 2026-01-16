using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Spine.Unity; 

public class SpineController : MonoBehaviour
{

    [SerializeField, ReadOnly] public SkeletonAnimation skeletonAnimation;

    #region Unity Method
        private void Awake()
        {
            skeletonAnimation = GetComponentInChildren<SkeletonAnimation>();
        } 
    #endregion

    #region Spine Controller
        public void PlayAnimation(int track, string name, bool loop){
            if (skeletonAnimation.AnimationName != name){
                skeletonAnimation.state.SetAnimation(track, name, loop);
            }
        }

        public void ClearTrack(int track){
            skeletonAnimation.state.ClearTrack(track);
        }

    #endregion
}