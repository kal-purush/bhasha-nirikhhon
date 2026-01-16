using System.Collections;
using UnityEngine;
using Visualization.UI;

namespace Visualization.Animation
{
    public class ConsoleRequestRead : ConsoleRequest
    {
        public string ReadValue { get; set; }

        public ConsoleRequestRead(string textToWrite) : base(textToWrite)
        {
            this.ReadValue = null;
        }

        public override void PerformRequest()
        {
            ConsolePanel.Instance.YieldOutput(WriteText);
            ConsolePanel.Instance.ActivateInputField(this);
        }
    }
}