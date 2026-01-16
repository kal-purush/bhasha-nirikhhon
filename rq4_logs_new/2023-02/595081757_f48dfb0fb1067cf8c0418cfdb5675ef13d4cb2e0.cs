using System.Collections;
using Animation;
using GameConstants;
using Lakeview_Interactive.QTE_System.Scripts.QTEs;
using QTESystem;
using UnityEngine;

namespace Puzzle
{
    public class QteHandler : MonoBehaviour
    {
        [SerializeField] private QTE qteComponent;
        [SerializeField] private GameObject ghostPosition;
        [SerializeField] private GameObject humanPosition;
        [SerializeField] private bool playPushAnimation;

        public QTE QteComponent { get; private set; }

        private void Awake()
        {
            QteComponent = qteComponent;
        }

        public IEnumerator StartSingleQteAnimation(bool isHuman)
        {
            if (isHuman)
            {
                Game.Input.HumanInputMode = InputMode.MovementLimited;
                qteComponent.SetCharType(CharacterType.Human);
                yield return StartCoroutine(LerpPosition(humanPosition.transform.position,
                    Game.Input.HumanPlayer.transform, Strings.LerpStepTrigger));
            }
            else if (!isHuman)
            {
                Game.Input.GhostInputMode = InputMode.MovementLimited;
                qteComponent.SetCharType(CharacterType.Ghost);
                yield return StartCoroutine(LerpPosition(ghostPosition.transform.position,
                    Game.Input.GhostPlayer.transform, Strings.PossessTrigger));
            }

            EnableQTE();
            qteComponent.BeginQTE();
        }

        public void StopQteAnimation()
        {
            if (Game.Input.HumanPlayer.TryGetComponent<AnimationsHandler>(out var animationsHandler))
            {
                animationsHandler.SetBoolParameter(Strings.PushingCouchParam, false);
                animationsHandler.TriggerParameter(Strings.PushFinishedTrigger);
            }
        }

        public IEnumerator StartCoopQteAnimations()
        {
            Game.Input.HumanInputMode = InputMode.MovementLimited;
            Game.Input.GhostInputMode = InputMode.MovementLimited;
            if (playPushAnimation)
            {
                if (Game.Input.HumanPlayer.TryGetComponent<AnimationsHandler>(out var humanAnimHandler))
                {
                    humanAnimHandler.SetBoolParameter(Strings.PushingCouchParam, true);
                }
            }

            yield return StartCoroutine(LerpBothPlayers());
            EnableQTE();
            qteComponent.BeginQTE();
        }

        private IEnumerator LerpBothPlayers()
        {
            PlayLerpAnimation(Game.Input.HumanPlayer.transform, Strings.LerpStepTrigger);
            PlayLerpAnimation(Game.Input.GhostPlayer.transform, Strings.PossessTrigger);

            const float lerpDuration = 1f;
            var currentTime = 0f;
            var startPosHuman = Game.Input.HumanPlayer.transform.position;
            var startPosGhost = Game.Input.GhostPlayer.transform.position;
            while (currentTime < lerpDuration)
            {
                currentTime += Time.deltaTime;
                var newHumanPos = Vector3.Lerp(startPosHuman, humanPosition.transform.position,
                    currentTime / lerpDuration);
                var newGhostPos = Vector3.Lerp(startPosGhost, ghostPosition.transform.position,
                    currentTime / lerpDuration);
                Game.Input.HumanPlayer.transform.position = newHumanPos;
                Game.Input.GhostPlayer.transform.position = newGhostPos;
                yield return null;
            }
        }

        private IEnumerator LerpPosition(Vector3 targetPosition, Transform targetTransform, string triggerParameter)
        {
            PlayLerpAnimation(targetTransform, triggerParameter);

            const float lerpDuration = 1f;
            var currentTime = 0f;
            var currentPos = targetTransform.position;
            while (currentTime < lerpDuration)
            {
                currentTime += Time.deltaTime;
                var newPos = Vector3.Lerp(currentPos, targetPosition, currentTime / lerpDuration);
                targetTransform.transform.position = newPos;
                yield return null;
            }
        }

        private static void PlayLerpAnimation(Component targetTransform, string triggerParameter)
        {
            if (targetTransform.TryGetComponent<AnimationsHandler>(out var humanAnimHandler))
            {
                humanAnimHandler.TriggerParameter(triggerParameter);
            }
            else if (targetTransform.TryGetComponent<AnimationsHandler>(out var ghostAnimHandler))
            {
                ghostAnimHandler.TriggerParameter(triggerParameter);
            }
        }

        public void EnableQTE()
        {
            qteComponent.gameObject.SetActive(true);
        }
    }
}