using UnityEngine;
using UnityEngine.UI;

namespace TwentyOneRemastered
{
    public class RulesMenu : MonoBehaviour
    {

        [SerializeField]
        Text textBox;

        public void SetTextBox(StringConstant textToSet)
        {
            textBox.text = textToSet.Value;
        }

} 
}