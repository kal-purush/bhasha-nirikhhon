using System.Collections.Generic;
using Interfaces;
using UnityEngine;

namespace GameInput
{
    internal class InputHandler : IPlayerInput
    {
        private readonly IReadOnlyDictionary<ButtonType, KeyCode> buttonMappings =
            new Dictionary<ButtonType, KeyCode>
            {
                { ButtonType.MoveUp, KeyCode.UpArrow },
                { ButtonType.MoveDown, KeyCode.DownArrow },
                { ButtonType.MoveLeft, KeyCode.LeftArrow },
                { ButtonType.MoveRight, KeyCode.RightArrow },
                { ButtonType.MoveUpGhost, KeyCode.W },
                { ButtonType.MoveDownGhost, KeyCode.S },
                { ButtonType.MoveLeftGhost, KeyCode.A },
                { ButtonType.MoveRightGhost, KeyCode.D },
                { ButtonType.JumpGhost, KeyCode.Space }
            };

        public IEnumerable<ButtonType> MappedButtons => buttonMappings.Keys;

        public bool GetButtonDown(ButtonType buttonType)
        {
            return Input.GetKeyDown(buttonMappings[buttonType]);
        }

        public bool GetButton(ButtonType buttonType)
        {
            return Input.GetKey(buttonMappings[buttonType]);
        }

        public bool GetButtonUp(ButtonType buttonType)
        {
            return Input.GetKeyUp(buttonMappings[buttonType]);
        }
    }
}