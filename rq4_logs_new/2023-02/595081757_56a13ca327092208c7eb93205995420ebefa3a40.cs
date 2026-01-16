using System.Collections;
using Audio;
using Events;
using Lakeview_Interactive.QTE_System.Scripts.QTEs;
using QTESystem;
using UnityEngine;

namespace Puzzle
{
    public class CoOpTriggerHandler : MonoBehaviour
    {
        public DefaultEvent eventToRaise;
        [HideInInspector] public bool ghostIsInteracting, humanIsInteracting, humanIsRightSide;

        [SerializeField] private GameObject qteObjectRight;
        [SerializeField] private GameObject qteObjectLeft;
        [SerializeField] private bool canActivate = true;
        [SerializeField] private DialogueSo waitingDialogue;

        private int _successCounter;

        public void GhostInteract()
        {
            StartCoroutine(TimedDialogue());
            CheckToStartQte();
        }

        public void HumanInteract()
        {
            StartCoroutine(TimedDialogue());

            CheckToStartQte();
        }

        private void CheckToStartQte()
        {
            if (!ghostIsInteracting || !humanIsInteracting || !canActivate)
            {
                return;
            }

            canActivate = false;
            qteObjectLeft.SetActive(true);
            qteObjectRight.SetActive(true);

            switch (humanIsRightSide)
            {
                case true:
                    qteObjectRight.GetComponentInChildren<MashingQTE>().SetCharType(CharacterType.Human);
                    qteObjectLeft.GetComponentInChildren<MashingQTE>().SetCharType(CharacterType.Ghost);
                    break;
                case false:
                    qteObjectRight.GetComponentInChildren<MashingQTE>().SetCharType(CharacterType.Ghost);
                    qteObjectLeft.GetComponentInChildren<MashingQTE>().SetCharType(CharacterType.Human);
                    break;
            }
        }

        public void ResetQteTimer(float secondsToWait)
        {
            StartCoroutine(QteCooldown(secondsToWait));
        }

        private IEnumerator QteCooldown(float secondsToWait)
        {
            humanIsInteracting = false;
            ghostIsInteracting = false;
            _successCounter = 0;

            qteObjectRight.SetActive(false);
            qteObjectLeft.SetActive(false);

            yield return new WaitForSeconds(secondsToWait);

            canActivate = true;
        }

        private void DisableTrigger()
        {
            Game.CharacterHandler.OnHumanInteract.RemoveListener(HumanInteract);
            Game.CharacterHandler.OnGhostInteract.RemoveListener(GhostInteract);
            gameObject.SetActive(false);
        }

        public void AddSuccess()
        {
            _successCounter++;
            if (_successCounter == 2)
            {
                CompleteEvent();
            }
        }

        public void CompleteEvent()
        {
            eventToRaise.RaiseEvent();
            DisableTrigger();
        }

        private IEnumerator TimedDialogue()
        {
            yield return new WaitForSeconds(5f);

            if (ghostIsInteracting && !humanIsInteracting)
            {
                DialogueCanvas.Instance.QueueDialogue(waitingDialogue);
            }

            if (humanIsInteracting && !ghostIsInteracting)
            {
                DialogueCanvas.Instance.QueueDialogue(waitingDialogue);
            }
        }
    }
}