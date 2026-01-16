using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using DG.Tweening;

namespace Assets.Scripts.Interfaces.OneOff
{
    public class DashTutorial : MonoBehaviour
    {
        [SerializeField] private TMPro.TMP_Text tutorialText;
        private bool done = false;

        private void Start()
        {
            tutorialText.color = new Color(tutorialText.color.r, tutorialText.color.g, tutorialText.color.b, 0);
        }

        private void Update()
        {
            if (done) return;

            var player = PlayerManager.Instance.Player;
            if (player && player.TryGetComponent(out Capabilities.Move move) && move._hasDash)
            {
                tutorialText.enabled = true;
                tutorialText.DOFade(1f, 2f).OnComplete(() => Destroy(gameObject));
                done = true;
            }
        }
    }
}