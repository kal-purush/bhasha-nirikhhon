using BrawlTCG_alpha.Logic.Cards;
using BrawlTCG_alpha.Visuals;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BrawlTCG_alpha.Logic
{
    internal class AttackManager
    {
        // Properties
        public bool SomeoneIsAttacking { get; private set; }
        public Attack SelectedAttack { get; private set; }
        
        // Fields
        public event Action<Player, ZoneTypes, bool> UI_EnableCardsInZone;

        // Methods
        public AttackManager(Action<Player, ZoneTypes, bool> ui_enableCardsInZone)
        {
            UI_EnableCardsInZone += ui_enableCardsInZone;
        }
        public void SetSelectedAttack(Attack a)
        {
            SelectedAttack = a;
        }
        public void SomeoneStartedAttacking()
        {
            SomeoneIsAttacking = true;
        }
        public void StopAttack(PlayerManager playerManager)
        {
            SomeoneIsAttacking = false;
            SelectedAttack = null;

            // enable your cards again
            UI_EnableCardsInZone.Invoke(playerManager.ActivePlayer, ZoneTypes.PlayingField, true);
        }
        public void StartAttack(Attack attack, PlayerManager playerManager)
        {
            // ideally disable cards here
            SomeoneIsAttacking = true;
            SelectedAttack = attack;

            // enable disable correct cards
            if (attack.FriendlyFire)
            {
                UI_EnableCardsInZone.Invoke(playerManager.ActivePlayer, ZoneTypes.PlayingField, true);
                UI_EnableCardsInZone.Invoke(playerManager.InactivePlayer, ZoneTypes.PlayingField, false);
            }
            else
            {
                UI_EnableCardsInZone.Invoke(playerManager.ActivePlayer, ZoneTypes.PlayingField, false);
                UI_EnableCardsInZone.Invoke(playerManager.InactivePlayer, ZoneTypes.PlayingField, true);
            }
        }
    }
}