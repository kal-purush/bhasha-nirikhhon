using Assets.Scripts.Screeps3D;
using Common;
using Screeps_API;
using UnityEngine;
using UnityEngine.UI;

namespace Screeps3D.Player
{
    public class CurrentUserView : MonoBehaviour
    {
        [SerializeField] private BadgeAndLabel badgeAndLabel = default;

        private string currentUser;

        private void Start()
        {
        }

        private void Update()
        {
            if (ScreepsAPI.IsConnected && ScreepsAPI.Me != null && currentUser != ScreepsAPI.Me.UserId)
            {
                currentUser = ScreepsAPI.Me.UserId;
                badgeAndLabel.SetOwner(ScreepsAPI.Me);
            }
        }
    }
}