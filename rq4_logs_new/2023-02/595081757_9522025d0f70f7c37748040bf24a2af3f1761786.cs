using UnityEngine;

namespace Puzzle.PuzzlePieces
{
    public class InputOnOff : MonoBehaviour
    {
        public enum PlayerType
        {
            Human,
            Ghost
        }
        
        public void InputEnableDisable(PlayerType player)
        {
            if (player == PlayerType.Ghost)
            {
                Game.CharacterHandler.GhostInputMode = Game.CharacterHandler.GhostInputMode == InputMode.Free ? InputMode.InQTE : InputMode.Free;
            }
            else
            {
                Game.CharacterHandler.HumanInputMode = Game.CharacterHandler.HumanInputMode == InputMode.Free ? InputMode.InQTE : InputMode.Free;
            }
        }
    }
}