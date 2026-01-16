using System;
using System.Collections;
using Audio;
using UnityEngine;

namespace Puzzle
{
    public class CoOpTrigger : MonoBehaviour
    {

        [SerializeField] private GameObject qteObjectH, qteObjectG;
        
        [SerializeField] private bool ghostIsInteracting, humanIsInteracting;

        [SerializeField] private bool canActivate = true;

        [SerializeField] private DialogueSo waitingDialogue;

        private int _successCounter = 0;
        
        private void OnTriggerEnter(Collider other)
        {
            //Human layer
            if (other.gameObject.layer == 9)
            {
                Game.CharacterHandler.OnHumanInteract.AddListener(HumanInteract);
            }
            
            //Ghost layer
            if (other.gameObject.layer == 6)
            {
                Game.CharacterHandler.OnGhostInteract.AddListener(GhostInteract);
            }
        }

        private void OnTriggerExit(Collider other)
        {
            //Human layer
            if (other.gameObject.layer == 9)
            {
                Game.CharacterHandler.OnHumanInteract.RemoveListener(HumanInteract);
            }
            
            //Ghost layer
            if (other.gameObject.layer == 6)
            {
                Game.CharacterHandler.OnGhostInteract.RemoveListener(GhostInteract);
            }
        }

        private void GhostInteract()
        {
            ghostIsInteracting = true;
            StartCoroutine(TimedDialogue());
            CheckToStartQte();
        }

        private void HumanInteract()
        {
            humanIsInteracting = true;
            StartCoroutine(TimedDialogue());
            CheckToStartQte();
        }

        private void CheckToStartQte()
        {
            if (ghostIsInteracting && humanIsInteracting && canActivate)
            {
                canActivate = false;
                qteObjectG.SetActive(true);
                qteObjectH.SetActive(true);
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
            
            qteObjectH.SetActive(false);
            qteObjectG.SetActive(false);
            
            yield return new WaitForSeconds(secondsToWait);

            canActivate = true;
        }
        
        private void DestroyTrigger()
        {
            Game.CharacterHandler.OnHumanInteract.RemoveListener(HumanInteract);
            Game.CharacterHandler.OnGhostInteract.RemoveListener(GhostInteract);
            Destroy(gameObject);
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
            GameObject.Find("PuzzleManager").GetComponent<CabinetPuzzle>().UpdateState(4);
            DestroyTrigger();
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