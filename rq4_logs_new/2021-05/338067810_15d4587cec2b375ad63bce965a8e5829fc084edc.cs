using UnityEngine;
using Assets.Scripts.Entity;

namespace Assets.Scripts
{
    class PlayerKillContainer : MonoBehaviour
    {
        public void OnTriggerEnter2D(Collider2D other)
        {
            if (other.TryGetComponent(out Player player))
                player.Kill();
        }
    }
}