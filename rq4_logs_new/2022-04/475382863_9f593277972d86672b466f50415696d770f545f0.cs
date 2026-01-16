using TMPro;
using UnityEngine;

namespace MVP_Proposal2.Scripts.Views
{
    public class MojoViewModel : MonoBehaviour
    {
        
        public virtual void Start()
        {
        
        }

        public void RaisePropertyChange(TextMeshProUGUI txt, string value)
        {
            txt.text = value;
        }
        
    }
}