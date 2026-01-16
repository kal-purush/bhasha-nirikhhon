///<summary>
/// Author:
///
///
///
///</summary>

using Millivolt.Player;
using UnityEngine;

namespace Millivolt
{
    public class PlayerComponents : MonoBehaviour
    {
        public PlayerController Controller { get; private set; }
        public PlayerInteraction Interaction { get; private set; }
        public PlayerStatus Status { get; private set; }
        public PlayerModel Model { get; private set; }
        public PlayerEmotion Emotion { get; private set; }

        public void Awake()
        {
            // get player references
            Transform player = FindObjectOfType<PlayerController>(true).transform;
            player.gameObject.SetActive(true);

            Controller = player.GetComponent<PlayerController>();
            Interaction = player.GetComponentInChildren<PlayerInteraction>();
            Status = player.GetComponentInChildren<PlayerStatus>();
            Model = player.GetComponentInChildren<PlayerModel>();
            Emotion = player.GetComponentInChildren<PlayerEmotion>();
        }
    }
}