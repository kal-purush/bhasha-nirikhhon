using System;
using UnityEngine;

namespace UnityTemplateProjects
{
    public class ExitDoor : MonoBehaviour
    {
        [SerializeField] private GameObject hint;

        private bool leavable;
        

        public Boolean GetLeavable()
        {
            return leavable;
        }

        public void ToggleHint(bool val)
        {
            hint.SetActive(val);
        }

        private void OnTriggerEnter(Collider other)
        {
            if (other.gameObject.CompareTag("Player"))
            {
                hint.SetActive(true);
                leavable = true;
            }
        }

        private void OnTriggerExit(Collider other)
        {
            if (other.gameObject.CompareTag("Player"))
            {
                hint.SetActive(false);
                leavable = false;
            }
        }
    }
}