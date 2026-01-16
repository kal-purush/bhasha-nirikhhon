using System.Collections;
using TMPro;
using UnityEngine;

namespace Audio
{
    public class DialogueCanvas : MonoBehaviour
    {
        public static DialogueCanvas Instance { get; private set; }

        private TextMeshProUGUI _dialogueTxt;

        private void Awake()
        {
            if (Instance != null && Instance != this)
            {
                Destroy(this);
            }
            else
            {
                Instance = this;
            }

            _dialogueTxt = GetComponentInChildren<TextMeshProUGUI>();
            _dialogueTxt.enabled = false;
        }

        public void SetText(DialogueSo dialogue)
        {
            _dialogueTxt.text = dialogue.dialogueText;
            StartCoroutine(TextTimer(dialogue));
        }

        private IEnumerator TextTimer(DialogueSo dia)
        {
            yield return new WaitForSeconds(.5f);
            _dialogueTxt.enabled = true;
            yield return new WaitForSeconds(dia.dialogueSound.clips[0].length);
            _dialogueTxt.enabled = false;
        }
    }
}