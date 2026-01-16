using Animation;
using GameConstants;
using UnityEngine;

namespace Puzzle.PuzzlePieces
{
    public class TriggerAnimation : MonoBehaviour
    {
        public void TriggerPlayOnceAnimation(GameObject target)
        {
            target.GetComponent<Animator>().SetBool(Strings.PlayOnceTrigger, true);
        }

        public void PlayUnPossessAnimation()
        {
            Game.Input.GhostPlayer.GetComponent<AnimationsHandler>().TriggerParameter(Strings.UnPossessTrigger);
        }
    }
}