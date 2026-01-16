using InitialPrefabs.Attributes.Messages;
using UnityEngine;

namespace InitialPrefabs.Examples {

    /// <summary>
    /// Shows some examples of using the MsgToggle.
    /// </summary>
    public class MsgToggleExample : MonoBehaviour {

        [MsgToggle("Should show when true!", MessageLevel.Info)]
        public bool toggleMsg = true;

        [MsgToggle("Should show when false!", true, MessageLevel.Warning)]
        public bool invertedToggleMsg = false;

        [MsgToggle]
        public string invalidMsgToggle = "An error would show below.";
    }
}