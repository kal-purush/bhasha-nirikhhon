using System;
using TMPro;
using UnityEngine;

namespace UnityTemplateProjects
{
    public class EndManager : MonoBehaviour
    {
        [SerializeField] private TextMeshProUGUI endText;
        [SerializeField] private TextMeshProUGUI time;

        private void Start()
        {
            endText.text = PlayerManager.Money <= 10000 ? "You Won!" : "You Lost";
            time.text = time.text = DateTime.Now.ToString("hh:mm tt");
        }
    }
}