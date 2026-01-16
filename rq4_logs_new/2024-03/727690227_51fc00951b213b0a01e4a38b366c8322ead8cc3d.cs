using System;
using UnityEngine;
using UnityEngine.UIElements;
using Visualization.Animation;
using Visualization.TODO;
using Visualization.UI.PopUps;

namespace Visualization.UI
{
    public class MediatorAddClassPopUp : Mediator
    {
        [SerializeField] private GameObject AddClassPopUp;

        public override void OnClicked(GameObject gameObject)
        {
            throw new System.NotImplementedException();
        }

        public void SetActiveAddClassPopUp(bool active)
        {
            AddClassPopUp.SetActive(active);
        }
        public void ActivateCreation()
        {
            AddClassPopUp.GetComponent<AddClassPopUp>().ActivateCreation();
        }

    }
}