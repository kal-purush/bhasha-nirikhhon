using Suhdo.Ultils;
using Unity.Mathematics;
using UnityEngine;

namespace Suhdo.Effects
{
    [RequireComponent(typeof(Animator), typeof(SpriteRenderer))]
    public class BaseFX : PoolableMonoBehaviour
    {
        protected Animator anim;

        protected virtual void Awake()
        {
            anim = GetComponent<Animator>();
            if(anim == null) Debug.LogWarning($"Not have animator on {transform.name}");
        }
        
        public override void OnObjectPoolReturn()
        {
            transform.parent = null;
            transform.localRotation = quaternion.identity;
            transform.position = Vector3.zero;
        }
        
        public virtual void OnFinishAnim()
        {
            Release();
        }
    }
}