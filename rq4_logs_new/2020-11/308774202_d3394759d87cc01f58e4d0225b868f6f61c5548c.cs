using UnityEngine;

namespace PersonalDevelopment
{
    public class BotMeleeAttackBehaviour : MonoBehaviour
    {
        [SerializeField] private float _pushBackHorizontal = 3f;
        [SerializeField] private float _pushBackVertical = 3f;
        
        #region Mono
        private void OnCollisionEnter(Collision other)
        {
            var collidedObject = other.gameObject;
            if (collidedObject.CompareTag("Player"))
            {
                var rigidBody = collidedObject.GetComponent<Rigidbody>();
                if (rigidBody)
                {
                    BumpPlayerBack(rigidBody);
                }

                var playerBehaviour = collidedObject.GetComponent<PlayerBehaviour>();
                if (playerBehaviour)
                {
                    DamagePlayerIfNeeded(playerBehaviour);
                }
            }
        }
        #endregion

        private void BumpPlayerBack(Rigidbody playerBody)
        {
            var direction = IsLeftHandSide(playerBody.gameObject) ? Vector3.left : Vector3.right;
            playerBody.velocity = Vector3.zero;
            playerBody.angularVelocity = Vector3.zero;
            playerBody.AddForce(direction * _pushBackHorizontal, ForceMode.Impulse);
            playerBody.AddForce(Vector3.up * _pushBackVertical, ForceMode.Impulse);
        }

        private void DamagePlayerIfNeeded(PlayerBehaviour player)
        {
            player.HurtPlayer();
        }

        private bool IsLeftHandSide(GameObject player)
        {
            return player.transform.position.x < transform.position.x;
        }
    }
}